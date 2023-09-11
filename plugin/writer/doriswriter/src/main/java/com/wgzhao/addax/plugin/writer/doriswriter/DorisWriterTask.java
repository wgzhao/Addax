/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.doriswriter;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_BYTE_SIZE;

public class DorisWriterTask {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterTask.class);
    private static final String DEFAULT_LABEL_PREFIX = "addax_doris_writer_";
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10 * 1000;
    private static final String DEFAULT_SEPARATOR = "|";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_FORMAT = "csv";
    private final Configuration configuration;
    private List<String> column;
    private long batchSize;
    private long batchByteSize;
    private DorisCodec rowCodec;
    private int batchNum = 0;
    private String database;
    private String table;
    private RequestConfig requestConfig;

    public DorisWriterTask(Configuration configuration) {
        this.configuration = configuration;
    }

    public void init() {
        initRequestConfig();
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        if ("csv".equalsIgnoreCase(configuration.getString(DorisKey.FORMAT, DEFAULT_FORMAT))) {
            this.rowCodec = new DorisCsvCodec(configuration.getList(DorisKey.COLUMN, String.class), DEFAULT_SEPARATOR);
        } else {
            this.rowCodec = new DorisJsonCodec(configuration.getList(DorisKey.COLUMN, String.class));
        }

        this.column = configuration.getList(Key.COLUMN, String.class);
        // 如果 column 填写的是 * ，直接设置为null，方便后续判断
        if (this.column != null && this.column.size() == 1 && "*".equals(this.column.get(0))) {
            this.column = null;
        }
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, 1024);
        this.batchByteSize = configuration.getLong(DorisKey.BATCH_BYTE_SIZE, DEFAULT_BATCH_BYTE_SIZE);
        this.database = conn.getString(DorisKey.DATABASE);
        this.table = conn.getString(DorisKey.TABLE);
    }

    public void startWrite(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector) {
        String lineDelimiter = configuration.getString(DorisKey.LINE_DELIMITER, DEFAULT_LINE_DELIMITER);

        try {
            DorisFlushBatch flushBatch = new DorisFlushBatch(lineDelimiter);
            Record record;
            long batchCount = 0L;
            long batchSize = 0L;
            while ((record = recordReceiver.getFromReader()) != null) {
                int len = record.getColumnNumber();
                // check column size
                if (this.column != null && len != this.column.size()) {
                    throw AddaxException.asAddaxException(
                            DorisWriterErrorCode.ILLEGAL_VALUE,
                            String.format("config writer column info error. because the column number of reader is :%s" +
                                    "and the column number of writer is:%s. please check your job config json", len, this.column.size())
                    );
                }
                // codec record
                final String recordStr = this.rowCodec.serialize(record);
                // put into buffer
                flushBatch.putData(recordStr);
                batchCount++;
                batchSize += recordStr.length();
                // trigger buffer
                if (batchCount >= this.batchSize || batchSize >= this.batchByteSize) {
                    flush(flushBatch);
                    // clear buffer
                    batchCount = 0L;
                    batchSize = 0L;
                    flushBatch = new DorisFlushBatch(lineDelimiter);
                }
            } // end of while
            // flush the last batch
            if (flushBatch.getSize() > 0) {
                flush(flushBatch);
            }
        } catch (Exception e) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    private void flush(DorisFlushBatch flushBatch)
            throws IOException {
        final String label = getStreamLoadLabel();
        flushBatch.setLabel(label);
        doStreamLoad(flushBatch);
    }

    private void initRequestConfig() {
        int connectTimeout = configuration.getInt(DorisKey.CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT_MS);
        requestConfig = RequestConfig.custom().setConnectTimeout(connectTimeout).build();
    }

    /**
     * execute doris stream load
     */
    public void doStreamLoad(final DorisFlushBatch flushData)
            throws IOException {
        long start = System.currentTimeMillis();
        final String host = this.getAvailableEndpoint();
        if (null == host) {
            throw new IOException("None of the load url can be connected.");
        }
        String loadUrl = host + "/api/" + database + "/" + table + "/_stream_load";
        loadUrl = urlDecode(loadUrl);
        // do http put request and get response
        final Map<String, Object> loadResult = this.doHttpPut(loadUrl, flushData);

        long cost = System.currentTimeMillis() - start;
        LOG.info("StreamLoad response: " + JSON.toJSONString(loadResult) + ", cost(ms): " + cost);
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to doris: unknown result status.");
        }
        if (loadResult.get(keyStatus).equals("Fail")) {
            throw new IOException("Failed to flush data to doris.\n" + JSON.toJSONString(loadResult));
        }
    }

    private Map<String, Object> doHttpPut(final String loadUrl, final DorisFlushBatch flushBatch)
            throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: %s, rows: %d",
                loadUrl, flushBatch.getSize(), flushBatch.getRows()));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
            @Override
            protected boolean isRedirectable(final String method) {
                return true;
            }

            @Override
            public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context)
                    throws ProtocolException {
                URI uri = this.getLocationURI(request, response, context);
                String method = request.getRequestLine().getMethod();
                if (method.equalsIgnoreCase("HEAD")) {
                    return new HttpHead(uri);
                } else if (method.equalsIgnoreCase("GET")) {
                    return new HttpGet(uri);
                } else {
                    int status = response.getStatusLine().getStatusCode();
                    return status == 307 ? RequestBuilder.copy(request).setUri(uri).build() : new HttpGet(uri);
                }
            }
        });

        try (final CloseableHttpClient httpclient = httpClientBuilder.build()) {
            final HttpPut httpPut = new HttpPut(loadUrl);
            final List<String> cols = configuration.getList(DorisKey.COLUMN, String.class);
            if (null != cols && !cols.isEmpty()) {
                httpPut.setHeader("columns", String.join(",", cols));
            }

            // put loadProps to http header
            final Map<String, Object> loadProps = configuration.getMap(DorisKey.LOAD_PROPS, null);
            if (null != loadProps) {
                for (final Map.Entry<String, Object> entry : loadProps.entrySet()) {
                    httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            String format = configuration.getString(DorisKey.FORMAT, DEFAULT_FORMAT);
            // set other required headers
            httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
            httpPut.setHeader(HttpHeaders.AUTHORIZATION,
                    this.getBasicAuthHeader(configuration.getString(DorisKey.USERNAME), configuration.getString(DorisKey.PASSWORD)));
            httpPut.setHeader("label", flushBatch.getLabel());
            httpPut.setHeader("format", format);
            httpPut.setHeader("line_delimiter", configuration.getString(DorisKey.LINE_DELIMITER, DEFAULT_LINE_DELIMITER));

            if ("csv".equalsIgnoreCase(format)) {
                httpPut.setHeader("column_separator", configuration.getString(DorisKey.FIELD_DELIMITER, DEFAULT_SEPARATOR));
            } else {
                httpPut.setHeader("read_json_by_line", "true");
                httpPut.setHeader("fuzzy_parse", "true");
            }

            // Use ByteArrayEntity instead of StringEntity to handle Chinese correctly
            httpPut.setEntity(new ByteArrayEntity(flushBatch.getData().toString().getBytes()));

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

    private String getBasicAuthHeader(final String username, final String password) {
        final String auth = username + ":" + password;
        final byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes());
        return "Basic " + new String(encodedAuth);
    }

    private String getStreamLoadLabel() {
        return DEFAULT_LABEL_PREFIX + UUID.randomUUID() + "_" + (batchNum++);
    }

    /**
     * loop to get target host
     *
     * @return the available endpoint
     */
    private String getAvailableEndpoint() {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        List<String> endpoints = conn.getList(DorisKey.ENDPOINT, String.class);
        for (String endpoint : endpoints) {
            if (this.tryHttpConnection(endpoint)) {
                return endpoint;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(final String host) {
        try {
            final URL url = new URL(host);
            final HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(1000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to connect to address:{} , Exception ={}", host, e);
            return false;
        }
    }

    public String urlDecode(String outBuffer) {
        String data = outBuffer;
        try {
            data = data.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
            data = data.replaceAll("\\+", "%2B");
            data = URLDecoder.decode(data, "utf-8");
        } catch (Exception e) {
            LOG.warn("urlDecode error: {}", e.getMessage());
        }
        return data;
    }
}
