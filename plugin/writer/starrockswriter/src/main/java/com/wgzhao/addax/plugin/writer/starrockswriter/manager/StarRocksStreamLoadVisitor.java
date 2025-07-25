/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.starrockswriter.manager;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.plugin.writer.starrockswriter.StarRocksWriterOptions;
import com.wgzhao.addax.plugin.writer.starrockswriter.row.StarRocksDelimiterParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StarRocksStreamLoadVisitor
{

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadVisitor.class);
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String RESULT_LABEL_TIMEOUT = "Publish Timeout";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";

    private static final String LABEL_STATE_VISIBLE = "VISIBLE";
    private static final String LABEL_STATE_COMMITTED = "COMMITTED";

    private final StarRocksWriterOptions writerOptions;
    private long pos;

    public StarRocksStreamLoadVisitor(StarRocksWriterOptions writerOptions)
    {
        this.writerOptions = writerOptions;
    }

    public void doStreamLoad(StarRocksFlushTuple flushData)
            throws IOException
    {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the host in `load_url` could be connected.");
        }
        String loadUrl = host +
                "/api/" +
                writerOptions.getDatabase() +
                "/" +
                writerOptions.getTable() +
                "/_stream_load";
        Map<String, Object> loadResult = doHttpPut(loadUrl, flushData.getLabel(), joinRows(flushData.getRows(), flushData.getBytes().intValue()));

        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to StarRocks: unknown result status.");
        }
        String resultStatus = loadResult.get(keyStatus).toString();
        if (RESULT_FAILED.equals(resultStatus)) {
            throw new IOException( "Failed to flush data to StarRocks.\n" + JSON.toJSONString(loadResult));
        }
        else if (RESULT_LABEL_EXISTED.equals(resultStatus) || RESULT_LABEL_TIMEOUT.equals(resultStatus)) {
            LOG.debug("StreamLoad response:\n{}", JSON.toJSONString(loadResult));
            // has to block-checking the state to get the final result
            checkLabelState(host, flushData.getLabel());
        }
    }

    private String getAvailableHost()
    {
        List<String> hostList = writerOptions.getLoadUrlList();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host)
    {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(1000);
            co.connect();
            co.disconnect();
            return true;
        }
        catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes)
    {
        if (StarRocksWriterOptions.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            byte[] lineDelimiter = StarRocksDelimiterParser.parse((String) props.get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (StarRocksWriterOptions.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
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

    private void checkLabelState(String host, String label)
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
                URIBuilder uriBuilder = new URIBuilder(host + "/api/" + writerOptions.getDatabase() + "/get_load_state");
                uriBuilder.addParameter("label", label);
                HttpGet httpGet = new HttpGet(uriBuilder.build().toString());
                httpGet.setHeader("Authorization", getBasicAuthHeader(writerOptions.getUsername(), writerOptions.getPassword()));
                httpGet.setHeader("Connection", "close");

                try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                    HttpEntity respEntity = getHttpEntity(resp);
                    if (respEntity == null) {
                        throw new IOException(String.format("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label[%s].\n", label), null);
                    }
                    Map<String, Object> result = (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
                    String labelState = (String) result.get("state");
                    if (null == labelState) {
                        throw new IOException(String.format("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label[%s]. response[%s]\n", label, EntityUtils.toString(respEntity)), null);
                    }
                    LOG.info("Checking label[{}] state[{}]\n", label, labelState);
                    switch (labelState) {
                        case LABEL_STATE_VISIBLE, LABEL_STATE_COMMITTED -> {
                            return;
                        }
                        case RESULT_LABEL_PREPARE -> {
                            continue;
                        }
                        case RESULT_LABEL_ABORTED -> throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                "label[%s] state[%s]\n", label, labelState), null, true);
                        default -> throw new IOException(String.format("Failed to flush data to StarRocks, Error " +
                                "label[%s] state[%s]\n", label, labelState), null);
                    }
                }
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data)
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
            List<String> cols = writerOptions.getColumns();
            if (null != cols && !cols.isEmpty() && StarRocksWriterOptions.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
                httpPut.setHeader("columns", cols.stream().map(f -> String.format("`%s`", f)).collect(Collectors.joining(",")));
            }
            if (null != writerOptions.getLoadProps()) {
                for (Map.Entry<String, Object> entry : writerOptions.getLoadProps().entrySet()) {
                    httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpPut.setHeader("Authorization", getBasicAuthHeader(writerOptions.getUsername(), writerOptions.getPassword()));
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
        return "Basic " + new String(encodedAuth, Charset.defaultCharset());
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
}
