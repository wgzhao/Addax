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
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DorisStreamLoadObserver
{
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadObserver.class);

    private final DorisKey options;
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LABEL_STATE_VISIBLE = "VISIBLE";
    private static final String LABEL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    public DorisStreamLoadObserver(DorisKey options)
    {
        this.options = options;
    }

    public String urlDecode(String outBuffer)
    {
        String data = outBuffer;
        try {
            data = data.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
            data = data.replaceAll("\\+", "%2B");
            data = URLDecoder.decode(data, "utf-8");
        }
        catch (Exception e) {
            LOG.error("Failed to decode url {}: {}", outBuffer, e.getLocalizedMessage());
        }
        return data;
    }

    public void streamLoad(WriterTuple data)
            throws Exception
    {
        String host = getLoadHost();
        if (host == null) {
            throw new IOException("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
        }
        String loadUrl = host + "/api/" + options.getDatabase() + "/" + options.getTable() + "/_stream_load";
        LOG.info("Start to join batch data: rows[{}] bytes[{}] label[{}].", data.getRows().size(), data.getBytes(), data.getLabel());
        loadUrl = urlDecode(loadUrl);
        Map<String, Object> loadResult = put(loadUrl, data.getLabel(), addRows(data.getRows(), data.getBytes().intValue()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to Doris: unknown result status.");
        }
        LOG.debug("StreamLoad response:{}", JSON.toJSONString(loadResult));
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            throw new IOException(
                    "Failed to flush data to Doris.\n" + JSON.toJSONString(loadResult)
            );
        }
        else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            LOG.debug("StreamLoad response:{}", JSON.toJSONString(loadResult));
            checkStreamLoadState(host, data.getLabel());
        }
    }

    private void checkStreamLoadState(String host, String label)
            throws IOException
    {
        int idx = 0;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            }
            catch (InterruptedException ex) {
                break;
            }
            try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
                HttpGet httpGet = new HttpGet(host + "/api/" + options.getDatabase() + "/get_load_state?label=" + label);
                httpGet.setHeader("Authorization", getBasicAuthHeader(options.getUsername(), options.getPassword()));
                httpGet.setHeader("Connection", "close");

                try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                    HttpEntity respEntity = getHttpEntity(resp);
                    if (respEntity == null) {
                        throw new IOException(String.format("Failed to flush data to Doris, Error " +
                                "could not get the final state of label[%s].\n", label), null);
                    }
                    Map<String, Object> result = (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
                    String labelState = (String) result.get("data");
                    if (null == labelState) {
                        throw new IOException(String.format("Failed to flush data to Doris, Error " +
                                "could not get the final state of label[%s]. response[%s]\n", label, EntityUtils.toString(respEntity)), null);
                    }
                    LOG.info("Checking label[{}] state[{}]\n", label, labelState);
                    switch (labelState) {
                        case LABEL_STATE_VISIBLE:
                        case LABEL_STATE_COMMITTED:
                            return;
                        case RESULT_LABEL_PREPARE:
                            continue;
                        case RESULT_LABEL_ABORTED:
                            throw new DorisWriterException(String.format("Failed to flush data to Doris, Error " +
                                    "label[%s] state[%s]\n", label, labelState), null, true);
                        case RESULT_LABEL_UNKNOWN:
                        default:
                            throw new IOException(String.format("Failed to flush data to Doris, Error " +
                                    "label[%s] state[%s]\n", label, labelState), null);
                    }
                }
            }
        }
    }

    private byte[] addRows(List<byte[]> rows, int totalBytes)
    {
        if (options.isCsvFormat()) {
            byte[] lineDelimiter = DelimiterParser.parse(options.getLineDelimiter(), "\n").getBytes(StandardCharsets.UTF_8);

            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (options.isJsonFormat()) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    private Map<String, Object> put(String loadUrl, String label, byte[] data)
            throws IOException
    {
        LOG.info("Executing stream load to: '{}', size: '{}'", loadUrl, data.length);
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy()
                {
                    @Override
                    protected boolean isRedirectable(String method)
                    {
                        return true;
                    }
                });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            httpPut.removeHeaders(HttpHeaders.CONTENT_LENGTH);
            httpPut.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
            List<String> cols = options.getColumns();
            if (null != cols && !cols.isEmpty() && options.isCsvFormat()) {
                httpPut.setHeader("columns", cols.stream().map(f -> String.format("`%s`", f)).collect(Collectors.joining(",")));
            }

            options.loadProps2Map().forEach(httpPut::setHeader);

            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("two_phase_commit", "false");
            httpPut.setHeader("Authorization", getBasicAuthHeader(options.getUsername(), options.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    return null;
                }
                return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private String getBasicAuthHeader(String username, String password)
    {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp)
    {
        int code = resp.getStatusLine().getStatusCode();
        if (200 != code) {
            LOG.warn("Request failed with code:{}", code);
            return null;
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            LOG.warn("Request failed with empty response.");
            return null;
        }
        return respEntity;
    }

    private String getLoadHost()
    {
        List<String> hostList = options.getLoadUrlList();
        Collections.shuffle(hostList);
        // get the first available host
        for (String host : hostList) {
            String uri = "http://" + host;
            if (checkConnection(uri)) {
                return uri;
            }
        }
        return null;
    }

    private boolean checkConnection(String host)
    {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(5000);
            co.connect();
            co.disconnect();
            return true;
        }
        catch (Exception e1) {
            LOG.warn("Failed to connect to host:{}", host);
            return false;
        }
    }
}
