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
import com.wgzhao.addax.core.element.Record;
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
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

/** Http Reader. */
public class HttpReader
        extends Reader
{
    /** The paging parameter names, the first page index and the number of records per page. */
    private record PageConfig(String sizeKey, String indexKey, int initialSize, int initialIndex)
    {
        static PageConfig defaultConfig()
        {
            return new PageConfig(HttpKey.PAGE_SIZE, HttpKey.PAGE_INDEX,
                    Task.DEFAULT_PAGE_SIZE, Task.DEFAULT_PAGE_INDEX);
        }
    }

    /** Auth endpoint configuration used to fetch token before reading business data. */
    private record AuthConfig(URI uri, String method, Map<String, Object> requestParams,
                              Map<String, String> headers, String resultKey,
                              String tokenHeader, String tokenPrefix)
    {
    }

    /**
     * One output column. A column is either a literal key, used by the wildcard mode where the keys
     * come from the response itself and may contain characters that JSONPath reads as syntax, or a
     * precompiled JSONPath expression from the column list.
     */
    private record Column(String name, JSONPath path)
    {
        static Column ofLiteral(String name)
        {
            return new Column(name, null);
        }

        static Column ofPath(String expression)
        {
            return new Column(expression, JSONPath.of(expression));
        }

        Object extract(JSONObject row)
        {
            return path == null ? row.get(name) : path.eval(row);
        }
    }

    /** Job. */
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

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final int DEFAULT_PAGE_INDEX = 1;
        private static final int DEFAULT_PAGE_SIZE = 20;
        private static final int DEFAULT_TIMEOUT_SEC = 60;
        private static final String DEFAULT_TOKEN_HEADER = "Authorization";
        private static final String DEFAULT_TOKEN_PREFIX = "Bearer ";
        private static final String DEFAULT_METHOD = "GET";
        private static final int DEFAULT_PREFETCH_PAGES = 1;
        /** Headers the http client refuses to set from user code, see HttpRequest.Builder#header. */
        private static final Set<String> RESTRICTED_HEADERS = Set.of("connection", "content-length", "expect", "host", "upgrade");

        // everything below is resolved once in init() and only read afterwards
        private URI baseUri;
        private String method;
        private String resultKey;
        private Map<String, Object> requestParams;
        private Map<String, String> headers;
        private Charset charset;
        private int timeoutSec;
        private String basicAuthHeader;
        private String token;
        private String tokenHeader;
        private String tokenPrefix;
        private InetSocketAddress proxyAddress;
        private String proxyUsername;
        private String proxyPassword;
        private boolean sslVerify;
        private boolean isPage;
        private int maxPages;
        private int prefetchPages;
        private PageConfig pageConfig;
        private boolean wildcard;
        private List<Column> columns;
        private AuthConfig authConfig;
        private HttpClient httpClient;

        @Override
        public void init()
        {
            Configuration conf = this.getPluginJobConf();
            this.method = conf.getString(HttpKey.METHOD, DEFAULT_METHOD).toUpperCase(Locale.ROOT);
            if (!"GET".equals(method) && !"POST".equals(method)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s] only supports GET and POST, but got [%s]".formatted(HttpKey.METHOD, method));
            }
            this.timeoutSec = conf.getInt(HttpKey.TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC);
            this.charset = resolveCharset(conf);
            this.resultKey = conf.getString(HttpKey.RESULT_KEY, "");
            this.baseUri = resolveUri(conf.getNecessaryValue(HttpKey.CONNECTION + "." + HttpKey.URL, REQUIRED_VALUE), HttpKey.URL);
            this.requestParams = new LinkedHashMap<>(conf.getMap(HttpKey.REQUEST_PARAMETERS, new LinkedHashMap<>()));
            this.headers = toStringMap(conf.getMap(HttpKey.HEADERS, new LinkedHashMap<>()));
            validateHeaders(this.headers);
            this.sslVerify = conf.getBool(HttpKey.SSL_VERIFY, false);
            this.isPage = conf.getBool(HttpKey.IS_PAGE, false);
            this.maxPages = conf.getInt(HttpKey.MAX_PAGES, 0);
            this.prefetchPages = conf.getInt(HttpKey.PREFETCH_PAGES, DEFAULT_PREFETCH_PAGES);
            if (maxPages < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s] must not be negative".formatted(HttpKey.MAX_PAGES));
            }
            if (prefetchPages < 1) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s] must be greater than 0".formatted(HttpKey.PREFETCH_PAGES));
            }
            this.pageConfig = resolvePageConfig(conf);
            resolveAuth(conf);
            resolveProxyConf(conf);
            resolveColumns(conf);
            initHttpClient();
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
                this.token = fetchToken();
                LOG.info("Token fetched from the authConfig endpoint");
            }

            if (isPage) {
                processPagedRequest(recordSender);
            }
            else {
                readOnce(recordSender);
            }
        }

        // ------------------------------------------------------------------ config

        private static Charset resolveCharset(Configuration conf)
        {
            String encoding = conf.getString(HttpKey.ENCODING, StandardCharsets.UTF_8.name());
            try {
                return Charset.forName(encoding);
            }
            catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Unsupported encoding [%s]".formatted(encoding));
            }
        }

        private static URI resolveUri(String value, String key)
        {
            URI uri;
            try {
                uri = new URI(value);
            }
            catch (URISyntaxException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s] is not a valid URI: %s".formatted(key, e.getMessage()));
            }

            if (uri.getHost() == null || (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme()))) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s] must be an absolute http(s) URL, but got [%s]".formatted(key, value));
            }
            return uri;
        }

        private static Map<String, String> toStringMap(Map<String, Object> source)
        {
            Map<String, String> result = new LinkedHashMap<>(source.size());
            source.forEach((k, v) -> result.put(k, v == null ? "" : String.valueOf(v)));
            return result;
        }

        private static void validateHeaders(Map<String, String> headers)
        {
            for (String name : headers.keySet()) {
                if (RESTRICTED_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The header [%s] cannot be set by the http client, remove it from [%s]".formatted(name, HttpKey.HEADERS));
                }
            }
        }

        private void resolveAuth(Configuration conf)
        {
            this.tokenHeader = DEFAULT_TOKEN_HEADER;
            this.tokenPrefix = DEFAULT_TOKEN_PREFIX;

            Configuration authConf = conf.getConfiguration(HttpKey.AUTH_CONFIG);
            if (authConf == null) {
                this.token = conf.getString(HttpKey.TOKEN, null);
                String username = conf.getString(HttpKey.USERNAME, null);
                String password = conf.getString(HttpKey.PASSWORD, null);
                if ((username == null) != (password == null)) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE,
                            "The parameters [%s] and [%s] must be configured together".formatted(HttpKey.USERNAME, HttpKey.PASSWORD));
                }
                if (username != null) {
                    // the credentials are fixed for the whole task, so the header is built once
                    this.basicAuthHeader = "Basic " + Base64.getEncoder()
                            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
                }
                return;
            }

            String authUrl = authConf.getString(HttpKey.URL, null);
            if (authUrl == null || authUrl.isBlank()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [authConfig.%s] is required when authConfig is configured".formatted(HttpKey.URL));
            }
            String authMethod = authConf.getString(HttpKey.METHOD, "POST").toUpperCase(Locale.ROOT);
            if (!"GET".equals(authMethod) && !"POST".equals(authMethod)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [authConfig.%s] only supports GET and POST, but got [%s]".formatted(HttpKey.METHOD, authMethod));
            }

            Map<String, String> authHeaders = toStringMap(authConf.getMap(HttpKey.HEADERS, new LinkedHashMap<>()));
            validateHeaders(authHeaders);

            this.authConfig = new AuthConfig(
                    resolveUri(authUrl, "authConfig." + HttpKey.URL),
                    authMethod,
                    new LinkedHashMap<>(authConf.getMap(HttpKey.REQUEST_PARAMETERS, new LinkedHashMap<>())),
                    authHeaders,
                    authConf.getString(HttpKey.RESULT_KEY, "token"),
                    authConf.getString(HttpKey.TOKEN_HEADER, DEFAULT_TOKEN_HEADER),
                    authConf.getString(HttpKey.TOKEN_PREFIX, DEFAULT_TOKEN_PREFIX));
            this.tokenHeader = authConfig.tokenHeader();
            this.tokenPrefix = authConfig.tokenPrefix() == null ? "" : authConfig.tokenPrefix();
        }

        private void resolveProxyConf(Configuration conf)
        {
            Configuration conn = conf.getConfiguration(HttpKey.CONNECTION);
            if (conn == null || conn.getString(HttpKey.PROXY, null) == null) {
                return;
            }

            Configuration proxyConf = conn.getConfiguration(HttpKey.PROXY);
            String host = proxyConf.getString(HttpKey.HOST, null);
            if (host == null || host.isBlank()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [connection.proxy.%s] is required when proxy is configured".formatted(HttpKey.HOST));
            }
            if (host.toLowerCase(Locale.ROOT).startsWith("socks")) {
                // the JDK http client cannot tunnel through a SOCKS proxy, it would talk plain HTTP to it
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "A SOCKS proxy is not supported by the http client, use an http proxy instead, but got [%s]".formatted(host));
            }
            URI proxyUri = resolveUri(host.contains("://") ? host : "http://" + host, "connection.proxy." + HttpKey.HOST);
            if (proxyUri.getPort() < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [connection.proxy.%s] must include the port, e.g. http://127.0.0.1:3128".formatted(HttpKey.HOST));
            }
            this.proxyAddress = new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort());

            String auth = proxyConf.getString(HttpKey.AUTH, null);
            if (auth == null) {
                return;
            }
            String[] parts = auth.split(":", 2);
            if (parts.length < 2 || parts[0].isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [connection.proxy.%s] must be in the form of username:password".formatted(HttpKey.AUTH));
            }
            this.proxyUsername = parts[0];
            this.proxyPassword = parts[1];
        }

        private void resolveColumns(Configuration conf)
        {
            List<String> configured;
            try {
                configured = conf.getList(HttpKey.COLUMN, String.class);
            }
            catch (ClassCastException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s] must be a list of strings".formatted(HttpKey.COLUMN));
            }
            if (configured.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [%s] is required".formatted(HttpKey.COLUMN));
            }

            this.wildcard = configured.size() == 1 && "*".equals(configured.get(0));
            if (wildcard) {
                this.columns = List.of();
                return;
            }

            List<Column> resolved = new ArrayList<>(configured.size());
            for (String expression : configured) {
                if (expression == null || expression.isBlank()) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The parameter [%s] must not contain an empty key".formatted(HttpKey.COLUMN));
                }
                try {
                    // parsing the expressions once takes the JSONPath parse out of the per-record path
                    resolved.add(Column.ofPath(expression));
                }
                catch (RuntimeException e) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "Invalid column expression [%s]: %s".formatted(expression, e.getMessage()));
                }
            }
            this.columns = List.copyOf(resolved);
        }

        private PageConfig resolvePageConfig(Configuration conf)
        {
            if (!isPage) {
                return PageConfig.defaultConfig();
            }

            Configuration pageParams = conf.getConfiguration(HttpKey.PAGE_PARAMS);
            if (pageParams == null) {
                return PageConfig.defaultConfig();
            }

            int pageSize = pageValue(pageParams, HttpKey.PAGE_SIZE, DEFAULT_PAGE_SIZE);
            int pageIndex = pageValue(pageParams, HttpKey.PAGE_INDEX, DEFAULT_PAGE_INDEX);
            if (pageSize < 1) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s.%s.value] must be greater than 0".formatted(HttpKey.PAGE_PARAMS, HttpKey.PAGE_SIZE));
            }
            if (pageIndex < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s.%s.value] must not be negative".formatted(HttpKey.PAGE_PARAMS, HttpKey.PAGE_INDEX));
            }

            return new PageConfig(
                    pageKey(pageParams, HttpKey.PAGE_SIZE, HttpKey.PAGE_SIZE),
                    pageKey(pageParams, HttpKey.PAGE_INDEX, HttpKey.PAGE_INDEX),
                    pageSize,
                    pageIndex);
        }

        /**
         * The pageParams sections are objects of the form {@code {"pageSize": {"key": "size", "value": 100}}};
         * a section that is not an object is a configuration mistake worth reporting precisely.
         */
        private static Configuration pageSection(Configuration pageParams, String section)
        {
            Object raw = pageParams.get(section);
            if (raw == null) {
                return null;
            }
            if (!(raw instanceof Map)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s.%s] must be an object holding [key] and [value], e.g. {\"key\": \"%s\", \"value\": 100}, but got [%s]"
                                .formatted(HttpKey.PAGE_PARAMS, section, section, raw));
            }
            return Configuration.from((Map<String, Object>) raw);
        }

        private static String pageKey(Configuration pageParams, String section, String defaultValue)
        {
            Configuration sectionConf = pageSection(pageParams, section);
            Object key = sectionConf == null ? null : sectionConf.get("key");
            return key == null || String.valueOf(key).isBlank() ? defaultValue : String.valueOf(key);
        }

        private static int pageValue(Configuration pageParams, String section, int defaultValue)
        {
            Configuration sectionConf = pageSection(pageParams, section);
            Object value = sectionConf == null ? null : sectionConf.get("value");
            return value == null ? defaultValue : parsePageInt(value, section);
        }

        private static int parsePageInt(Object value, String section)
        {
            try {
                return Integer.parseInt(String.valueOf(value).trim());
            }
            catch (NumberFormatException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [%s.%s.value] must be an integer, but got [%s]".formatted(HttpKey.PAGE_PARAMS, section, value));
            }
        }

        private void initHttpClient()
        {
            HttpClient.Builder builder = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(timeoutSec));

            if (proxyAddress != null) {
                builder.proxy(ProxySelector.of(proxyAddress));
                if (proxyUsername != null) {
                    builder.authenticator(new ProxyAuthenticator(proxyUsername, proxyPassword));
                }
            }

            if (!sslVerify && (isHttps(baseUri) || (authConfig != null && isHttps(authConfig.uri())))) {
                try {
                    // note: an empty identification algorithm turns the hostname check off
                    SSLParameters sslParameters = new SSLParameters();
                    sslParameters.setEndpointIdentificationAlgorithm("");
                    builder.sslContext(createInsecureSslContext()).sslParameters(sslParameters);
                }
                catch (NoSuchAlgorithmException | KeyManagementException e) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "Failed to initialize the SSL context: " + e.getMessage());
                }
                LOG.warn("TLS certificate and hostname verification are disabled, set [{}]=true to enable them", HttpKey.SSL_VERIFY);
            }

            httpClient = builder.build();
        }

        private static boolean isHttps(URI uri)
        {
            return uri != null && "https".equalsIgnoreCase(uri.getScheme());
        }

        private SSLContext createInsecureSslContext()
                throws NoSuchAlgorithmException, KeyManagementException
        {
            TrustManager[] trustAllCerts = new TrustManager[] {
                    new InsecureTrustManager()
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            return sslContext;
        }

        // ------------------------------------------------------------------ reading

        private void readOnce(RecordSender recordSender)
        {
            URI requestUri = buildRequestUri(baseUri, method, requestParams);
            sendPage(await(requestUri, dispatch(requestUri, method, requestParams, headers, true)), recordSender);
        }

        private void processPagedRequest(RecordSender recordSender)
        {
            int pageSize = pageConfig.initialSize();
            PagePipeline pipeline = new PagePipeline(pageConfig.initialIndex());
            int pages = 0;
            int records = 0;
            String previousBody = null;
            int previousCount = -1;

            try {
                while (true) {
                    PendingPage page = pipeline.poll();
                    if (page == null) {
                        if (pipeline.limitReached()) {
                            LOG.warn("Stop paging at the configured [{}]={}; raise it to read the remaining pages", HttpKey.MAX_PAGES, maxPages);
                        }
                        break;
                    }

                    pages++;
                    String body = await(page.requestUri(), page.response());
                    // keep the network busy while this page is parsed and written, otherwise the round
                    // trip of every single page is spent waiting
                    pipeline.fill();

                    // an endpoint that ignores the paging parameters answers with the same full page forever,
                    // the already read records would be written again and again
                    if (body.equals(previousBody) && previousCount >= pageSize) {
                        LOG.warn("Page {} returned exactly the same payload as page {}, the endpoint is probably ignoring the paging parameters; stop paging after {} record(s)", page.pageIndex(), page.pageIndex() - 1, records);
                        break;
                    }

                    int count = sendPage(body, recordSender);
                    records += count;
                    if (count < pageSize) {
                        break;
                    }
                    previousBody = body;
                    previousCount = count;
                }
            }
            finally {
                pipeline.cancel();
            }

            LOG.info("Paging finished: {} page(s), {} record(s)", pages, records);
        }

        /** A page that is being requested, the response is read when the pipeline hands it out. */
        private record PendingPage(int pageIndex, URI requestUri, CompletableFuture<HttpResponse<String>> response)
        {
        }

        /**
         * Requests the following pages while the current one is parsed and written, so that the round trip
         * of a page is not spent waiting. The responses are handed out in the order the pages were
         * requested, and the ones that are left over when the read ends are cancelled.
         */
        private final class PagePipeline
        {
            private final Deque<PendingPage> inFlight = new ArrayDeque<>();
            private int nextPageIndex;
            private int requested;

            PagePipeline(int firstPageIndex)
            {
                this.nextPageIndex = firstPageIndex;
                fill();
            }

            void fill()
            {
                while (inFlight.size() < prefetchPages && (maxPages == 0 || requested < maxPages)) {
                    inFlight.add(startPage(nextPageIndex++));
                    requested++;
                }
            }

            PendingPage poll()
            {
                return inFlight.poll();
            }

            boolean limitReached()
            {
                return maxPages > 0 && requested >= maxPages;
            }

            void cancel()
            {
                for (PendingPage page : inFlight) {
                    page.response().cancel(true);
                }
                inFlight.clear();
            }
        }

        private PendingPage startPage(int pageIndex)
        {
            Map<String, Object> params = new LinkedHashMap<>(requestParams);
            params.put(pageConfig.sizeKey(), pageConfig.initialSize());
            params.put(pageConfig.indexKey(), pageIndex);
            URI requestUri = buildRequestUri(baseUri, method, params);
            return new PendingPage(pageIndex, requestUri, dispatch(requestUri, method, params, headers, true));
        }

        private String fetchToken()
        {
            String body = executeRequest(authConfig.uri(), authConfig.method(),
                    authConfig.requestParams(), authConfig.headers(), false);
            Object payload = JSON.parse(body);
            if (payload == null) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, "The authConfig endpoint returned an empty response body");
            }

            String resultPath = authConfig.resultKey();
            Object tokenValue = extractByPath(payload, resultPath);
            if (tokenValue == null || tokenValue.toString().isBlank()) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "Failed to fetch token from authConfig. Result key '%s' not found or empty".formatted(resultPath));
            }
            return tokenValue.toString();
        }

        /**
         * Parse a response body and send every record in it to the writer.
         *
         * @return the number of records the body held
         */
        private int sendPage(String body, RecordSender recordSender)
        {
            JSONArray jsonArray = extractArray(body);
            if (jsonArray.isEmpty()) {
                return 0;
            }

            List<Column> pageColumns = wildcard ? wildcardColumns(jsonArray) : columns;
            for (int i = 0; i < jsonArray.size(); i++) {
                Object element = jsonArray.get(i);
                if (!(element instanceof JSONObject row)) {
                    throw AddaxException.asAddaxException(RUNTIME_ERROR,
                            "Element %d of the response array is [%s], but a JSON object is required".formatted(i, describe(element)));
                }

                Record record = recordSender.createRecord();
                for (Column column : pageColumns) {
                    Object value = column.extract(row);
                    record.addColumn(new StringColumn(value == null ? null : value.toString()));
                }
                recordSender.sendToWriter(record);
            }
            return jsonArray.size();
        }

        private JSONArray extractArray(String body)
        {
            Object parsed = JSON.parse(body);
            if (parsed == null) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, "The response body is empty, no record can be extracted");
            }

            Object jsonData = resultKey.isEmpty() ? parsed : extractByPath(parsed, resultKey);
            if (jsonData instanceof JSONArray array) {
                return array;
            }
            if (jsonData instanceof JSONObject object) {
                JSONArray array = new JSONArray(1);
                array.add(object);
                return array;
            }
            if (jsonData == null) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "The result key [%s] does not exist in the response".formatted(resultKey));
            }
            throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    "The result key [%s] points to [%s], but a JSON object or array is required".formatted(resultKey, describe(jsonData)));
        }

        /**
         * The wildcard column list is the union of the keys of every record in the page, in first seen
         * order. Taking the keys of the first record only would silently drop the fields the first
         * record happens to miss.
         */
        private List<Column> wildcardColumns(JSONArray jsonArray)
        {
            Set<String> keys = new LinkedHashSet<>();
            Set<String> extraKeys = new LinkedHashSet<>();
            Set<String> firstKeys = null;

            for (int i = 0; i < jsonArray.size(); i++) {
                Object element = jsonArray.get(i);
                if (!(element instanceof JSONObject row)) {
                    throw AddaxException.asAddaxException(RUNTIME_ERROR,
                            "The parameter [column] is [*], which requires every element of the response array to be a JSON object, "
                                    + "but element %d is [%s]".formatted(i, describe(element)));
                }
                if (i == 0) {
                    firstKeys = row.keySet();
                }
                else {
                    for (String key : row.keySet()) {
                        if (!firstKeys.contains(key)) {
                            extraKeys.add(key);
                        }
                    }
                }
                keys.addAll(row.keySet());
            }

            if (!extraKeys.isEmpty()) {
                LOG.warn("The records of this page do not share the same set of keys; the union {} is used and a value missing from a record is written as NULL. Keys absent from the first record: {}",
                        keys, extraKeys);
            }

            List<Column> resolved = new ArrayList<>(keys.size());
            for (String key : keys) {
                resolved.add(Column.ofLiteral(key));
            }
            return resolved;
        }

        private static String describe(Object value)
        {
            return value == null ? "null" : value.getClass().getSimpleName();
        }

        // ------------------------------------------------------------------ http

        /** Send one request without waiting for it, so that the following page can be requested in parallel. */
        private CompletableFuture<HttpResponse<String>> dispatch(URI requestUri, String requestMethod, Map<String, ?> requestParams,
                Map<String, String> requestHeaders, boolean withAuth)
        {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().uri(requestUri);
            requestHeaders.forEach(requestBuilder::header);

            if (withAuth) {
                if (basicAuthHeader != null) {
                    requestBuilder.setHeader("Authorization", basicAuthHeader);
                }
                if (token != null) {
                    requestBuilder.setHeader(tokenHeader, tokenPrefix + token);
                }
            }

            switch (requestMethod) {
                case "GET" -> requestBuilder.GET();
                case "POST" -> {
                    // a single empty key carries a raw request body: ["123", "456"], [1, 2, 3] or "123,456"
                    Object rawBody = requestParams.get("");
                    String jsonBody = rawBody != null ? String.valueOf(rawBody).trim() : JSON.toJSONString(requestParams);
                    if (requestHeaders.keySet().stream().noneMatch(k -> "Content-Type".equalsIgnoreCase(k))) {
                        requestBuilder.setHeader("Content-Type", "application/json");
                    }
                    requestBuilder.POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8));
                }
                default -> throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "Unsupported HTTP method: " + requestMethod);
            }

            if (LOG.isInfoEnabled()) {
                LOG.info("Requesting: {}", requestUri);
            }
            return httpClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofString(charset));
        }

        private String executeRequest(URI targetUri, String requestMethod, Map<String, ?> requestParams,
                Map<String, String> requestHeaders, boolean withAuth)
        {
            URI requestUri = buildRequestUri(targetUri, requestMethod, requestParams);
            return await(requestUri, dispatch(requestUri, requestMethod, requestParams, requestHeaders, withAuth));
        }

        /**
         * Wait for a dispatched request and return its body. The timeout is applied to the whole exchange:
         * the one of {@link HttpRequest} only covers the response headers, a server that stalls the body
         * would otherwise hold the task forever.
         */
        private String await(URI requestUri, CompletableFuture<HttpResponse<String>> pending)
        {
            try {
                return checkStatus(requestUri, pending.get(timeoutSec, TimeUnit.SECONDS));
            }
            catch (TimeoutException e) {
                pending.cancel(true);
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "HTTP request to %s did not complete within %d second(s)".formatted(requestUri, timeoutSec));
            }
            catch (InterruptedException e) {
                pending.cancel(true);
                // keep the shutdown signal visible to the framework
                Thread.currentThread().interrupt();
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "HTTP request to %s was interrupted".formatted(requestUri));
            }
            catch (ExecutionException e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "HTTP request to %s failed: %s".formatted(requestUri, rootMessage(e.getCause())));
            }
            catch (CancellationException e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "HTTP request to %s was cancelled".formatted(requestUri));
            }
        }

        private static String rootMessage(Throwable cause)
        {
            if (cause == null) {
                return "unknown error";
            }
            return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        }

        private static String checkStatus(URI requestUri, HttpResponse<String> response)
        {
            int status = response.statusCode();
            if (status < 200 || status >= 300) {
                // redirects are not followed, so a 3xx means the data was not read at all
                String location = response.headers().firstValue("Location")
                        .map(value -> ", Location: " + value)
                        .orElse("");
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        "The request to %s failed with status code %d%s".formatted(requestUri, status, location));
            }
            return response.body();
        }

        /** GET carries the parameters in the query string, POST in the request body. */
        private static URI buildRequestUri(URI target, String requestMethod, Map<String, ?> requestParams)
        {
            return buildUri(target, "GET".equals(requestMethod) ? requestParams : Map.of());
        }

        /**
         * Append the request parameters to the query string of the target URI, keeping the query
         * string the user configured.
         */
        private static URI buildUri(URI target, Map<String, ?> requestParams)
        {
            List<String> pairs = new ArrayList<>();
            if (target.getRawQuery() != null) {
                for (String pair : target.getRawQuery().split("&")) {
                    if (pair.isEmpty()) {
                        continue;
                    }
                    // a request parameter of the same name replaces the one configured in the url
                    String name = decode(pair.split("=", 2)[0]);
                    if (name.isEmpty() || requestParams.containsKey(name)) {
                        continue;
                    }
                    pairs.add(pair);
                }
            }
            for (Map.Entry<String, ?> entry : requestParams.entrySet()) {
                if (entry.getKey().isEmpty()) {
                    // the empty key is the raw body placeholder, it is not a real parameter
                    continue;
                }
                pairs.add(encode(entry.getKey()) + '=' + encode(String.valueOf(entry.getValue())));
            }

            StringBuilder uri = new StringBuilder()
                    .append(target.getScheme()).append("://")
                    .append(target.getRawAuthority())
                    .append(target.getRawPath() == null ? "" : target.getRawPath());
            if (!pairs.isEmpty()) {
                uri.append('?').append(String.join("&", pairs));
            }
            return URI.create(uri.toString());
        }

        /** Character set of an url that is already percent-encoded; the '+' of a form body means a space. */
        private static String decode(String value)
        {
            try {
                return URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
            catch (IllegalArgumentException e) {
                return value;
            }
        }

        private static String encode(String value)
        {
            // URLEncoder encodes for form bodies, where a space is '+'; '%20' stays unambiguous in a query string
            return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
        }

        private static Object extractByPath(Object root, String path)
        {
            return JSONPath.eval(root, path.startsWith("$") ? path : "$." + path);
        }

        /**
         * The http client shares one Authenticator between proxy challenges and server challenges,
         * so answering everything with the proxy credentials would leak them to the endpoint.
         */
        private static final class ProxyAuthenticator
                extends Authenticator
        {
            private final String username;
            private final String password;

            ProxyAuthenticator(String username, String password)
            {
                this.username = username;
                this.password = password;
            }

            @Override
            protected PasswordAuthentication getPasswordAuthentication()
            {
                if (getRequestorType() != RequestorType.PROXY) {
                    return null;
                }
                return new PasswordAuthentication(username, password.toCharArray());
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
