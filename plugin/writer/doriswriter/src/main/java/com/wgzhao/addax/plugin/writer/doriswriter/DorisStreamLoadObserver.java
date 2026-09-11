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
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Doris Stream Load Observer. */
public class DorisStreamLoadObserver
        implements Closeable
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

    private final String basicAuthHeader;
    private final List<String> hosts;
    private final AtomicInteger hostIndex = new AtomicInteger(0);
    private final Map<String, Long> hostCooldownUntil = new ConcurrentHashMap<>();
    private final RequestConfig requestConfig;
    private final CloseableHttpClient httpClient;

    /** Dorisstreamloadobserver. */
    public DorisStreamLoadObserver(DorisKey options)
    {
        this.options = options;
        this.basicAuthHeader = "Basic " + new String(Base64.encodeBase64(
                (options.getUsername() + ":" + options.getPassword()).getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
        this.hosts = new ArrayList<>();
        for (String host : options.getLoadUrlList()) {
            this.hosts.add(host.startsWith("http://") || host.startsWith("https://") ? host : "http://" + host);
        }
        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(options.getConnectTimeout())
                .setSocketTimeout(options.getSocketTimeout())
                .setConnectionRequestTimeout(options.getConnectionRequestTimeout())
                .setRedirectsEnabled(true)
                .build();

        // One pooled client for the whole task: a client per batch pays a TCP handshake
        // (plus a TLS handshake for https) for every single stream load.  The pool also
        // ends up holding a connection per backend the FE redirects us to, so the limits
        // are kept well above the number of configured hosts to never block on a lease.
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setDefaultMaxPerRoute(Math.max(8, this.hosts.size() * 2));
        connManager.setMaxTotal(Math.max(32, this.hosts.size() * 8));
        this.httpClient = HttpClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(this.requestConfig)
                .setRedirectStrategy(new DefaultRedirectStrategy()
                {
                    @Override
                    protected boolean isRedirectable(String method)
                    {
                        // Doris answers a stream load on the FE with a redirect to a BE
                        return true;
                    }
                })
                .build();
    }

    /** Urldecode. */
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

    /** Streamload. */
    public void streamLoad(WriterTuple data)
            throws Exception
    {
        String host = getLoadHost();
        if (host == null) {
            throw new IOException("load_url cannot be empty, please check your configuration.");
        }
        String loadUrl = host + "/api/" + options.getDatabase() + "/" + options.getTable() + "/_stream_load";
        LOG.debug("Start to join batch data: rows[{}] bytes[{}] label[{}].", data.getRows().size(), data.getBytes(), data.getLabel());
        loadUrl = urlDecode(loadUrl);
        Map<String, Object> loadResult;
        try {
            loadResult = put(loadUrl, data.getLabel(), addRows(data.getRows(), data.getBytes().intValue()));
        }
        catch (IOException e) {
            // connection level failure: stop sending the next batches to this host for a while
            markHostFailure(host);
            throw e;
        }
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            markHostFailure(host);
            throw new IOException("Unable to flush data to Doris: unknown result status.");
        }
        LOG.debug("StreamLoad response:{}", JSON.toJSONString(loadResult));
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            markHostFailure(host);
            throw new IOException(
                    "Failed to flush data to Doris.\n" + JSON.toJSONString(loadResult)
            );
        }
        else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            checkStreamLoadState(host, data.getLabel());
        }
        else {
            markHostSuccess(host);
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
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while checking the state of label " + label, ex);
            }
            HttpGet httpGet = new HttpGet(host + "/api/" + options.getDatabase() + "/get_load_state?label=" + label);
            httpGet.setHeader("Authorization", basicAuthHeader);
            httpGet.setConfig(requestConfig);

            try (CloseableHttpResponse resp = httpClient.execute(httpGet)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    throw new IOException(String.format("Failed to flush data to Doris, Error " +
                            "could not get the final state of label[%s].\n", label), null);
                }
                Map<String, Object> result = (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity, StandardCharsets.UTF_8));
                String labelState = result == null ? null : (String) result.get("data");
                if (null == labelState) {
                    throw new IOException(String.format("Failed to flush data to Doris, Error " +
                            "could not get the final state of label[%s]. response[%s]\n", label, JSON.toJSONString(result)), null);
                }
                LOG.info("Checking label[{}] state[{}]\n", label, labelState);
                switch (labelState) {
                    case LABEL_STATE_VISIBLE:
                    case LABEL_STATE_COMMITTED:
                        markHostSuccess(host);
                        return;
                    case RESULT_LABEL_PREPARE:
                        continue;
                    case RESULT_LABEL_ABORTED:
                        markHostFailure(host);
                        throw new DorisWriterException(String.format("Failed to flush data to Doris, Error " +
                                "label[%s] state[%s]\n", label, labelState), null, true);
                    case RESULT_LABEL_UNKNOWN:
                    default:
                        markHostFailure(host);
                        throw new IOException(String.format("Failed to flush data to Doris, Error " +
                                "label[%s] state[%s]\n", label, labelState), null);
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
        LOG.debug("Executing stream load to: '{}', size: '{}'", loadUrl, data.length);
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
        httpPut.setHeader("Authorization", basicAuthHeader);
        httpPut.setEntity(new ByteArrayEntity(data));
        httpPut.setConfig(requestConfig);
        try (CloseableHttpResponse resp = httpClient.execute(httpPut)) {
            HttpEntity respEntity = getHttpEntity(resp);
            if (respEntity == null) {
                return null;
            }
            return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity, StandardCharsets.UTF_8));
        }
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp)
            throws IOException
    {
        int code = resp.getStatusLine().getStatusCode();
        if (code < 200 || code >= 300) {
            String body = resp.getEntity() == null ? ""
                    : EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
            throw new IOException("Stream load request failed with code=" + code + ", response=" + body);
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            LOG.warn("Request succeeded but the response body is empty.");
            return null;
        }
        return respEntity;
    }

    /**
     * Picks the next usable load host in round-robin order, skipping the hosts that are
     * cooling down after a failure.  Previously every batch shuffled the host list and
     * opened a throw-away TCP connection just to probe the first host.
     */
    private String getLoadHost()
    {
        if (hosts.isEmpty()) {
            return null;
        }
        long now = System.currentTimeMillis();
        int size = hosts.size();
        int start = Math.floorMod(hostIndex.getAndIncrement(), size);
        for (int i = 0; i < size; i++) {
            String host = hosts.get((start + i) % size);
            Long coolingUntil = hostCooldownUntil.get(host);
            if (coolingUntil == null || coolingUntil <= now) {
                return host;
            }
        }
        // no host is usable right now, trying one is still better than failing the batch
        // outright: a connection error is retried by the caller against another host
        String fallback = hosts.get(start);
        LOG.warn("All Doris load hosts are cooling down, fallback to {}", fallback);
        return fallback;
    }

    private void markHostFailure(String host)
    {
        hostCooldownUntil.put(host, System.currentTimeMillis() + options.getHostCooldownMs());
    }

    private void markHostSuccess(String host)
    {
        hostCooldownUntil.remove(host);
    }

    @Override
    public void close()
            throws IOException
    {
        httpClient.close();
    }
}
