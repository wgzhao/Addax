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

package com.wgzhao.addax.plugin.reader.httpreader;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONWriter;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.fluent.Content;
import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

public class HttpReader
        extends Reader
{
    // Use record for page configuration
    private record PageConfig(String sizeKey, String indexKey, int initialSize, int initialIndex)
    {
        static PageConfig defaultConfig()
        {
            return new PageConfig(HttpKey.PAGE_SIZE, HttpKey.PAGE_INDEX,
                    Task.DEFAULT_PAGE_SIZE, Task.DEFAULT_PAGE_INDEX);
        }
    }

    public static class Job
            extends Reader.Job
    {
        private Configuration originConfig = null;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> result = new ArrayList<>();
            result.add(this.originConfig);
            return result;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private static final int DEFAULT_PAGE_INDEX = 1;
        private static final int DEFAULT_PAGE_SIZE = 20;
        private Configuration readerSliceConfig = null;
        private URIBuilder uriBuilder;
        private String username;
        private String password;
        private String token;
        private BasicCredentialsProvider credsProvider;
        private HttpHost proxy = null;
        private Request request;
        private String method;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.username = readerSliceConfig.getString(HttpKey.USERNAME, null);
            this.password = readerSliceConfig.getString(HttpKey.PASSWORD, null);
            this.token = readerSliceConfig.getString(HttpKey.TOKEN, null);
            this.method = readerSliceConfig.getString(HttpKey.METHOD, "get");
            Configuration conn = readerSliceConfig.getConfiguration(HttpKey.CONNECTION);
            uriBuilder = new URIBuilder(URI.create(conn.getString(HttpKey.URL)));
            if (conn.getString(HttpKey.PROXY, null) != null) {
                // set proxy
                setProxy(conn.getConfiguration(HttpKey.PROXY));
            }

            Map<String, Object> requestParams = readerSliceConfig.getMap(HttpKey.REQUEST_PARAMETERS, new HashMap<>());
            requestParams.forEach((k, v) -> uriBuilder.setParameter(k, v.toString()));
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            var isPage = readerSliceConfig.getBool(HttpKey.IS_PAGE, false);
            if (isPage) {
                processPagedRequest(recordSender);
            }
            else {
                getRecords(recordSender);
            }
        }

        private void processPagedRequest(RecordSender recordSender)
        {
            var pageConfig = getPageConfig();
            var pageSize = pageConfig.initialSize();
            var pageIndex = pageConfig.initialIndex();

            uriBuilder.setParameter(pageConfig.sizeKey(), String.valueOf(pageSize));
            while (true) {
                uriBuilder.setParameter(pageConfig.indexKey(), String.valueOf(pageIndex));
                var realPageSize = getRecords(recordSender);
                if (realPageSize < pageSize) {
                    break;
                }
                pageIndex++;
            }
        }

        private PageConfig getPageConfig()
        {
            var pageParams = readerSliceConfig.getConfiguration(HttpKey.PAGE_PARAMS);
            if (pageParams == null) {
                return PageConfig.defaultConfig();
            }

            var indexConfig = pageParams.getString(HttpKey.PAGE_INDEX) != null ?
                    pageParams.getMap(HttpKey.PAGE_INDEX) : Map.of();
            var sizeConfig = pageParams.getString(HttpKey.PAGE_SIZE) != null ?
                    pageParams.getMap(HttpKey.PAGE_SIZE) : Map.of();

            return new PageConfig(
                    (String) sizeConfig.getOrDefault("key", HttpKey.PAGE_SIZE),
                    (String) indexConfig.getOrDefault("key", HttpKey.PAGE_INDEX),
                    Integer.parseInt(sizeConfig.getOrDefault("value", DEFAULT_PAGE_SIZE).toString()),
                    Integer.parseInt(indexConfig.getOrDefault("value", DEFAULT_PAGE_INDEX).toString())
            );
        }

        private int getRecords(RecordSender recordSender)
        {
            var charset = Charset.forName(readerSliceConfig.getString(HttpKey.ENCODING, StandardCharsets.UTF_8.name()));

            try {
                LOG.info("Requesting: {}", uriBuilder.build());
                request = Request.create(method, uriBuilder.build());
            }
            catch (URISyntaxException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "Invalid URI: %s".formatted(e.getMessage()));
            }

            var body = createCloseableHttpResponse().asString(charset);
            var resultKey = readerSliceConfig.getString(HttpKey.RESULT_KEY, "");
            var jsonData = resultKey.isEmpty() ?
                    JSON.parse(body) :
                    JSONPath.eval(JSON.parse(body), "$." + resultKey);
            JSONArray jsonArray = null;
            if (jsonData instanceof JSONArray) {
                jsonArray = JSON.parseArray(JSONObject.toJSONString(jsonData, JSONWriter.Feature.WriteMapNullValue));
            }
            else if (jsonData instanceof JSONObject) {
                jsonArray = new JSONArray();
                jsonArray.add(jsonData);
            }

            if (jsonArray == null || jsonArray.isEmpty()) {
                return 0;
            }

            var columns = readerSliceConfig.getList(HttpKey.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [%s] is required".formatted(HttpKey.COLUMN));
            }

            // Handle column extraction
            if (columns.size() == 1 && "*".equals(columns.get(0))) {
                columns = new ArrayList<>(jsonArray.getJSONObject(0).keySet());
            }

            return processJsonArray(jsonArray, columns, recordSender);
        }

        private int processJsonArray(JSONArray jsonArray, List<String> columns, RecordSender recordSender)
        {
            for (int i = 0; i < jsonArray.size(); i++) {
                var record = recordSender.createRecord();
                var jsonObject = jsonArray.getJSONObject(i);

                columns.forEach(column -> {
                    var value = JSONPath.eval(jsonObject, column);
                    record.addColumn(new StringColumn(value != null ? value.toString() : null));
                });

                recordSender.sendToWriter(record);
            }
            return jsonArray.size();
        }

        private void setProxy(Configuration proxyConf)
        {
            URI host;
            try {
                host = new URI(proxyConf.getString(HttpKey.HOST));
            }
            catch (URISyntaxException e) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, e.getMessage()
                );
            }

            this.proxy = new HttpHost(host.getScheme(), host.getHost(), host.getPort());
            if (proxyConf.getString(HttpKey.AUTH, null) != null) {
                String[] auth = proxyConf.getString(HttpKey.AUTH).split(":");
                credsProvider = new BasicCredentialsProvider();
                credsProvider.setCredentials(
                        new AuthScope(proxy),
                        new UsernamePasswordCredentials(auth[0], auth[1].toCharArray())
                );
            }
        }

        private Content createCloseableHttpResponse()
        {
            readerSliceConfig.getMap(HttpKey.HEADERS, new HashMap<>())
                    .forEach((k, v) -> request.addHeader(k, v.toString()));
            try (CloseableHttpClient httpClient = createCloseableHttpClient()) {
                return Executor.newInstance(httpClient)
                        .execute(request)
                        .returnContent();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private CloseableHttpClient createCloseableHttpClient()
        {
            HttpClientBuilder httpClientBuilder = HttpClients.custom();
            if (proxy != null) {
                httpClientBuilder.setProxy(proxy);
            }
            if (this.password != null) {
                // setup BasicAuth
                // Create the authentication scope
                HttpHost target = new HttpHost(uriBuilder.getScheme(), uriBuilder.getHost(), uriBuilder.getPort());
                // Create credential pair
                UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password.toCharArray());
                // Inject the credentials
                if (credsProvider == null) {
                    credsProvider =  new BasicCredentialsProvider();
                }
                credsProvider.setCredentials(new AuthScope(target), credentials);
            }

            if (this.token != null) {
                request.addHeader("Authorization", "Bearer " + token);
            }

            if (credsProvider != null) {
                httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
            }
            Map<String, Object> headers = readerSliceConfig.getMap(HttpKey.HEADERS, new HashMap<>());
            headers.forEach((k, v) -> request.addHeader(k, v.toString()));
            if (Objects.equals(uriBuilder.getScheme(), "https")) {
                PoolingHttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                        .setTlsSocketStrategy(customTlsStrategy())
                        .build();
                return httpClientBuilder
                        .setConnectionManager(cm)
                        .build();
            }
            else {
                return httpClientBuilder.build();
            }
        }

        private DefaultClientTlsStrategy customTlsStrategy()
        {
            TrustStrategy trustStrategy = (x509Certificates, s) -> true;
            try {
                // use the TrustSelfSignedStrategy to allow Self Signed Certificates
                SSLContext ssl = SSLContexts.custom()
                        .loadTrustMaterial(null, trustStrategy)
                        .build();
                // ignore hostname verification
                return new DefaultClientTlsStrategy(ssl, NoopHostnameVerifier.INSTANCE);
            }
            catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, e.getMessage()
                );
            }
        }
    }
}
