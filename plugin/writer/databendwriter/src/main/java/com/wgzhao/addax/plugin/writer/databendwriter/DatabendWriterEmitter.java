/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.writer.databendwriter;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Used to load batch of rows to Databend using stream load
 */
public class DatabendWriterEmitter
{
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterEmitter.class);
    private final Configuration conf;
    private static final int DEFAULT_CONNECT_TIMEOUT = -1;

    private RequestConfig requestConfig;

    public DatabendWriterEmitter(final Configuration conf)
    {
        this.conf = conf;
        initRequestConfig();
    }

    private void initRequestConfig()
    {
        int connectTimeout = conf.getInt(DatabendKey.CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
        requestConfig = RequestConfig.custom().setConnectTimeout(connectTimeout).build();
    }

    /**
     * execute databend stream load
     */
    public void doStreamLoad(final DatabendFlushBatch flushData)
            throws IOException
    {
        long start = System.currentTimeMillis();
        List<Object> connList = conf.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        final String host = conn.getNecessaryValue(DatabendKey.ENDPOINT, DatabendWriterErrorCode.REQUIRED_VALUE);
        final String database = conn.getNecessaryValue(DatabendKey.DATABASE, DatabendWriterErrorCode.REQUIRED_VALUE);
        final String table = conn.getNecessaryValue(DatabendKey.TABLE, DatabendWriterErrorCode.REQUIRED_VALUE);
        if (null == host) {
            throw new IOException("None of the load url can be connected.");
        }

        final String loadUrl = host + "v1/streaming_load";

        // do http put request and get response
        final Map<String, Object> loadResult = this.doHttpPut(loadUrl, flushData, database, table);

        long cost = System.currentTimeMillis() - start;
        LOG.info("StreamLoad response: " + JSON.toJSONString(loadResult) + ", cost(ms): " + cost);
        final String keyStatus = "state";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            LOG.error("Data is [" + flushData.getData().toString() + "]");
            throw new IOException("Unable to flush data to databend: unknown result status.");
        }
        if (!loadResult.get(keyStatus).equals("SUCCESS")) {
            throw new IOException("Failed to flush data to databend.\n" + JSON.toJSONString(loadResult));
        }
    }

    private Map<String, Object> doHttpPut(final String loadUrl, final DatabendFlushBatch flushBatch, String database, String table)
            throws IOException
    {
        LOG.info(String.format("Executing stream load to: '%s', size: %s, rows: %d",
                loadUrl, flushBatch.getSize(), flushBatch.getRows()));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy()
        {
            @Override
            protected boolean isRedirectable(final String method)
            {
                return true;
            }

            @Override
            public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context)
                    throws ProtocolException
            {
                URI uri = this.getLocationURI(request, response, context);
                String method = request.getRequestLine().getMethod();
                if (method.equalsIgnoreCase("HEAD")) {
                    return new HttpHead(uri);
                }
                else if (method.equalsIgnoreCase("GET")) {
                    return new HttpGet(uri);
                }
                else {
                    int status = response.getStatusLine().getStatusCode();
                    return status == 307 ? RequestBuilder.copy(request).setUri(uri).build() : new HttpGet(uri);
                }
            }
        });

        try (final CloseableHttpClient httpclient = httpClientBuilder.build()) {
            final HttpPut httpPut = new HttpPut(loadUrl);

            // set other required headers
            httpPut.setHeader(HttpHeaders.AUTHORIZATION,
                    this.getBasicAuthHeader(conf.getString(DatabendKey.USERNAME), conf.getString(DatabendKey.PASSWORD)));
            String format = conf.getString(Key.FORMAT);
            if (!"csv".equalsIgnoreCase(format)) {
                throw new IOException(String.format("Now only support csv format. But set [%s].", format));
            }

            String field_delimiter = conf.getString(DatabendKey.FIELD_DELIMITER, "\t");
            String line_delimiter = conf.getString(DatabendKey.LINE_DELIMITER, "\n");
            final List<String> cols = conf.getList(DatabendKey.COLUMN, String.class);
            if (null != cols && !cols.isEmpty()) {
                if (cols.size() == 1 && Objects.equals(cols.get(0), "*")) {
//                    String insert = String.format("insert into %s.%s format %s ", database, table, format);
//                    LOG.info("The insert is [" + insert + "]");
                    String insert = String.format("insert into %s.%s file_format = (type = '%s' field_delimiter = '%s' record_delimiter = '%s') ", database, table, format, field_delimiter, line_delimiter);
                    httpPut.setHeader("insert_sql", insert);
                } else {
                    String columns = "(" + String.join(",", cols) + ")";
//                    httpPut.setHeader("insert_sql", String.format("insert into %s.%s %s format %s ", database, table, columns, format));
                    String insert = String.format("insert into %s.%s %s file_format = (type = '%s' field_delimiter = '%s' record_delimiter = '%s') ", database, table, columns, format, field_delimiter, line_delimiter);
                    httpPut.setHeader("insert_sql", insert);
                }
            } else {
                throw new IOException("COLUMN is empty");
            }

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            builder.addBinaryBody("AddaxUpFile", flushBatch.getData().toString().getBytes(), ContentType.DEFAULT_BINARY, "addax_up_file");

            HttpEntity entity = builder.build();
            httpPut.setEntity(entity);

            httpPut.setConfig(requestConfig);

            try (final CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                final int code = resp.getStatusLine().getStatusCode();
                if (HttpStatus.SC_OK != code) {
                    LOG.warn("Request failed with code:{}", code);
                    return null;
                }
                final HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private String getBasicAuthHeader(final String username, final String password)
    {
        final String auth = username + ":" + password;
        final byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes());
        return "Basic " + new String(encodedAuth);
    }
}
