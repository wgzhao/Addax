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
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;

import java.io.IOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

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

    // Auth endpoint configuration used to fetch token before reading business data.
    private record AuthConfig(URI uri, String method, Map<String, String> requestParams,
                              Map<String, Object> headers, String resultKey,
                              String tokenHeader, String tokenPrefix)
    {
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
        private static final int DEFAULT_TIMEOUT_SEC = 60;
        private static final String DEFAULT_TOKEN_HEADER = "Authorization";
        private static final String DEFAULT_TOKEN_PREFIX = "Bearer ";

        private Configuration readerSliceConfig = null;
        private URI baseUri;
        private final Map<String, String> queryParams = new HashMap<>();
        private String username;
        private String password;
        private String token;
        private InetSocketAddress proxyAddress;
        private String proxyUsername;
        private String proxyPassword;
        private String method;
        private HttpClient httpClient;
        private int timeout;
        private AuthConfig authConfig;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.username = readerSliceConfig.getString(HttpKey.USERNAME, null);
            this.password = readerSliceConfig.getString(HttpKey.PASSWORD, null);
            this.token = readerSliceConfig.getString(HttpKey.TOKEN, null);
            this.method = readerSliceConfig.getString(HttpKey.METHOD, "get");
            this.timeout = readerSliceConfig.getInt(HttpKey.TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC);
            Configuration conn = readerSliceConfig.getConfiguration(HttpKey.CONNECTION);
            this.baseUri = URI.create(conn.getString(HttpKey.URL));

            Configuration authConf = readerSliceConfig.getConfiguration(HttpKey.AUTH_CONFIG);
            if (authConf != null) {
                this.authConfig = parseAuthConfig(authConf);
            }

            if (conn.getString(HttpKey.PROXY, null) != null) {
                setProxy(conn.getConfiguration(HttpKey.PROXY));
            }

            Map<String, Object> requestParams = readerSliceConfig.getMap(HttpKey.REQUEST_PARAMETERS, new HashMap<>());
            requestParams.forEach((k, v) -> queryParams.put(k, v.toString()));

            initHttpClient();
        }

        private URI buildUri()
        {
            return buildUri(baseUri, method, queryParams);
        }

        private AuthConfig parseAuthConfig(Configuration authConf)
        {
            String authUrl = authConf.getString(HttpKey.URL, null);
            if (authUrl == null || authUrl.isBlank()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [authConfig.url] is required when authConfig is configured");
            }

            String authMethod = authConf.getString(HttpKey.METHOD, "POST");
            Map<String, String> authParams = new HashMap<>();
            authConf.getMap(HttpKey.REQUEST_PARAMETERS, new HashMap<>())
                    .forEach((k, v) -> authParams.put(k, String.valueOf(v)));

            String resultKey = authConf.getString(HttpKey.RESULT_KEY, "token");
            String tokenHeader = authConf.getString(HttpKey.TOKEN_HEADER, DEFAULT_TOKEN_HEADER);
            String tokenPrefix = authConf.getString(HttpKey.TOKEN_PREFIX, DEFAULT_TOKEN_PREFIX);

            return new AuthConfig(
                    URI.create(authUrl),
                    authMethod,
                    authParams,
                    authConf.getMap(HttpKey.HEADERS, new HashMap<>()),
                    resultKey,
                    tokenHeader,
                    tokenPrefix
            );
        }

        private URI buildUri(URI targetUri, String requestMethod, Map<String, String> requestParams)
        {
            StringBuilder uriBuilder = new StringBuilder();
            uriBuilder.append(targetUri.getScheme()).append("://")
                    .append(targetUri.getAuthority())
                    .append(targetUri.getPath() == null ? "" : targetUri.getPath());

            if ("GET".equalsIgnoreCase(requestMethod)) {
                Map<String, String> allParams = new HashMap<>();
                if (targetUri.getQuery() != null) {
                    for (String param : targetUri.getQuery().split("&")) {
                        String[] parts = param.split("=", 2);
                        if (parts.length == 2) {
                            allParams.put(parts[0], parts[1]);
                        }
                    }
                }
                allParams.putAll(requestParams);

                if (!allParams.isEmpty()) {
                    uriBuilder.append('?');
                    allParams.forEach((k, v) -> {
                        if (uriBuilder.charAt(uriBuilder.length() - 1) != '?') {
                            uriBuilder.append('&');
                        }
                        uriBuilder.append(k).append('=').append(v);
                    });
                }
            }

            return URI.create(uriBuilder.toString());
        }

        private void initHttpClient()
        {
            HttpClient.Builder builder = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(timeout));

            // Configure proxy if needed
            if (proxyAddress != null) {
                builder.proxy(ProxySelector.of(proxyAddress));
                if (proxyUsername != null && proxyPassword != null) {
                    builder.authenticator(new Authenticator()
                    {
                        @Override
                        protected PasswordAuthentication getPasswordAuthentication()
                        {
                            return new PasswordAuthentication(proxyUsername, proxyPassword.toCharArray());
                        }
                    });
                }
            }

            // Configure SSL for HTTPS
            if (Objects.equals(baseUri.getScheme(), "https")) {
                try {

                    // 配置 SSL 参数以禁用主机名验证
                    SSLParameters sslParameters = new SSLParameters();
                    sslParameters.setEndpointIdentificationAlgorithm("");

                    builder.sslContext(createInsecureSslContext())
                            .sslParameters(sslParameters);

                    LOG.warn("SSL certificate verification and hostname verification are disabled. This is not recommended for production use.");
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "Failed to initialize SSL context: " + e.getMessage());
                }
            }

            httpClient = builder.build();
        }

        private SSLContext createInsecureSslContext()
                throws NoSuchAlgorithmException, KeyManagementException
        {
            TrustManager[] trustAllCerts = new TrustManager[] {
                    new InsecureTrustManager()
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            return sslContext;
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            if (authConfig != null) {
                this.token = fetchTokenFromAuthConfig();
                LOG.info("Token fetched successfully from authConfig.");
            }

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

            queryParams.put(pageConfig.sizeKey(), String.valueOf(pageSize));
            while (true) {
                queryParams.put(pageConfig.indexKey(), String.valueOf(pageIndex));
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

        private String fetchTokenFromAuthConfig()
        {
            String responseBody = executeRequest(
                    authConfig.uri(),
                    authConfig.method(),
                    authConfig.requestParams(),
                    authConfig.headers(),
                    false
            );

            Object authPayload = JSON.parse(responseBody);
            String resultPath = authConfig.resultKey();
            Object tokenValue = resultPath.startsWith("$")
                    ? JSONPath.eval(authPayload, resultPath)
                    : JSONPath.eval(authPayload, "$." + resultPath);

            if (tokenValue == null || tokenValue.toString().isBlank()) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "Failed to fetch token from authConfig. Result key '%s' not found or empty".formatted(resultPath));
            }
            return tokenValue.toString();
        }

        private String executeRequest()
        {
            return executeRequest(
                    baseUri,
                    method,
                    queryParams,
                    readerSliceConfig.getMap(HttpKey.HEADERS, new HashMap<>()),
                    true
            );
        }

        private String executeRequest(URI targetUri, String requestMethod, Map<String, String> requestParams,
                Map<String, Object> headers, boolean withAuth)
        {
            var charset = Charset.forName(readerSliceConfig.getString(HttpKey.ENCODING, StandardCharsets.UTF_8.name()));
            URI requestUri = buildUri(targetUri, requestMethod, requestParams);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(requestUri)
                    .timeout(Duration.ofMinutes(2));
            // Add headers
            headers.forEach((k, v) -> requestBuilder.header(k, v.toString()));

            // Add authentication
            if (withAuth && username != null && password != null) {
                String auth = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
                requestBuilder.setHeader("Authorization", "Basic " + auth);
            }
            if (withAuth && token != null) {
                String tokenHeader = authConfig == null ? DEFAULT_TOKEN_HEADER : authConfig.tokenHeader();
                String tokenPrefix = authConfig == null ? DEFAULT_TOKEN_PREFIX : authConfig.tokenPrefix();
                requestBuilder.setHeader(tokenHeader, (tokenPrefix == null ? "" : tokenPrefix) + token);
            }

            // Set method and handle body for POST
            String jsonBody;
            if ("POST".equalsIgnoreCase(requestMethod)) {
                if (requestParams.containsKey("")) {
                    // maybe just one parameter, like ["123","456"], or [1,2,3], or "123,456"
                    jsonBody = requestParams.get("").trim();
                }
                else {
                    jsonBody = JSON.toJSONString(requestParams);
                }
                requestBuilder.header("Content-Type", "application/json");
                requestBuilder.POST(HttpRequest.BodyPublishers.ofString(jsonBody));
            }
            else if ("GET".equalsIgnoreCase(requestMethod)) {
                requestBuilder.GET();
            }
            else {
                throw new IllegalArgumentException("Unsupported HTTP method: " + requestMethod);
            }

            try {
                HttpResponse<String> response = httpClient.send(requestBuilder.build(),
                        HttpResponse.BodyHandlers.ofString(charset));
                if (response.statusCode() >= 400) {
                    throw new IOException("HTTP request failed with status code: " + response.statusCode());
                }
                return response.body();
            }
            catch (InterruptedException e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, "HTTP request was interrupted: %s".formatted(e.getMessage()));
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, "HTTP request failed: %s".formatted(e.getMessage()));
            }
        }

        private int getRecords(RecordSender recordSender)
        {
            LOG.info("Requesting: {}", buildUri());
            String body = executeRequest();
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
            try {
                URI host = new URI(proxyConf.getString(HttpKey.HOST));
                this.proxyAddress = new InetSocketAddress(host.getHost(), host.getPort());

                if (proxyConf.getString(HttpKey.AUTH, null) != null) {
                    String[] auth = proxyConf.getString(HttpKey.AUTH).split(":");
                    this.proxyUsername = auth[0];
                    this.proxyPassword = auth[1];
                }
            }
            catch (URISyntaxException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, e.getMessage());
            }
        }

        private static class InsecureTrustManager
                extends X509ExtendedTrustManager
        {
            @Override
            public X509Certificate[] getAcceptedIssuers()
            {
                return new X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType, Socket socket) {}

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType, Socket socket) {}

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType, SSLEngine engine) {}

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType, SSLEngine engine) {}
        }
    }
}
