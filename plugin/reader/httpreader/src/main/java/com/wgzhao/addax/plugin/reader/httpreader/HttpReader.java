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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;
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

public class HttpReader
        extends Reader
{
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
        private Configuration readerSliceConfig = null;
        private URIBuilder uriBuilder;
        private HttpHost proxy = null;
        private HttpClientContext context = null;
        private String username;
        private String password;
        private String proxyAuth;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.username = readerSliceConfig.getString(Key.USERNAME, null);
            this.password = readerSliceConfig.getString(Key.PASSWORD, null);
            Configuration conn =
                    readerSliceConfig.getListConfiguration(Key.CONNECTION).get(0);
            uriBuilder = new URIBuilder(URI.create(conn.getString(Key.URL)));

            if (conn.getString(Key.PROXY, null) != null) {
                // set proxy
                createProxy(conn.getConfiguration(Key.PROXY));
            }

            Map<String, Object> requestParams =
                    readerSliceConfig.getMap(Key.REQUEST_PARAMETERS, new HashMap<>());
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
            String method = readerSliceConfig.getString(Key.METHOD, "get");
            CloseableHttpResponse response;
            try {
                response = createCloseableHttpResponse(method);
                StatusLine statusLine = response.getStatusLine();
                if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                    response.getEntity().getContent().close();
                    throw new IOException(statusLine.getReasonPhrase());
                }
                HttpEntity entity = response.getEntity();

                String encoding = readerSliceConfig.getString(Key.ENCODING, null);
                Charset charset;
                if (encoding != null) {
                    charset = Charset.forName(encoding);
                }
                else {
                    charset = StandardCharsets.UTF_8;
                }

                String json = EntityUtils.toString(entity, charset);
                JSONArray jsonArray = null;
                String key = readerSliceConfig.get(Key.RESULT_KEY, null);
                Object object;
                if (key != null) {
                    object = JSON.parseObject(json).get(key);
                }
                else {
                    object = JSON.parse(json);
                }
                // 需要判断返回的结果仅仅是一条记录还是多条记录，如果是一条记录，则是一个map
                // 否则是一个array
                if (object instanceof JSONArray) {
                    // 有空值的情况下, toString会过滤掉，所以不能简单的使用 object.toString()方式
                    // https://github.com/wgzhao/DataX/issues/171
                    jsonArray = JSON.parseArray(JSONObject.toJSONString(object, SerializerFeature.WriteMapNullValue));
                }
                else if (object instanceof JSONObject) {
                    jsonArray = new JSONArray();
                    jsonArray.add(object);
                }
                if (jsonArray == null || jsonArray.isEmpty()) {
                    // empty result
                    return;
                }

                List<String> columns = readerSliceConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            HttpReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set."
                    );
                }
                Record record;
                JSONObject jsonObject = jsonArray.getJSONObject(0);
                if (columns.size() == 1 && "*".equals(columns.get(0))) {
                    // 没有给定key的情况下，提取JSON的第一层key作为字段处理
                    columns.remove(0);
                    for (Object obj : JSONPath.keySet(jsonObject, "/")) {
                        columns.add(obj.toString());
                    }
                }
                // first, check key exists or not ?
                for (String k : columns) {
                    if (!jsonObject.containsKey(k)) {
                        throw AddaxException.asAddaxException(
                                HttpReaderErrorCode.ILLEGAL_VALUE,
                                "您尝试从结果中获取key为 '" + k + "'的结果，但实际结果中不存在该key值"
                        );
                    }
                }
                for (int i = 0; i < jsonArray.size(); i++) {
                    jsonObject = jsonArray.getJSONObject(i);
                    record = recordSender.createRecord();
                    for (String k : columns) {
                        Object v = JSONPath.eval(jsonObject, k);
                        if (v == null) {
                            record.addColumn(new StringColumn(null));
                        }
                        else {
                            record.addColumn(new StringColumn(v.toString()));
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            }

            catch (URISyntaxException | IOException e) {
                throw AddaxException.asAddaxException(
                        HttpReaderErrorCode.ILLEGAL_VALUE, e.getMessage()
                );
            }
        }

        private void createProxy(Configuration proxyConf)
        {
            String host = proxyConf.getString(Key.HOST);
            this.proxyAuth = proxyConf.getString(Key.AUTH);
            URI uri = URI.create(host);
            this.context = HttpClientContext.create();
            this.context.setAttribute("proxy", uri);


        }

        private CloseableHttpResponse createCloseableHttpResponse(String method)
                throws URISyntaxException, IOException
        {
            Map<String, Object> headers = readerSliceConfig.getMap(Key.HEADERS, new HashMap<>());
            CloseableHttpClient httpClient;
            CloseableHttpResponse response;

            if ("get".equalsIgnoreCase(method)) {
                HttpGet request = new HttpGet(uriBuilder.build());
                headers.forEach((k, v) -> request.setHeader(k, v.toString()));
                httpClient = createCloseableHttpClient();
                response = httpClient.execute(request, this.context);
            }
            else if ("post".equalsIgnoreCase(method)) {
                HttpPost request = new HttpPost(uriBuilder.build());
                headers.forEach((k, v) -> request.setHeader(k, v.toString()));
                httpClient = createCloseableHttpClient();
                response = httpClient.execute(request, this.context);
            }
            else {
                throw AddaxException.asAddaxException(
                        HttpReaderErrorCode.ILLEGAL_VALUE, "不支持的请求模式: " + method
                );
            }
            return response;
        }

        private CloseableHttpClient createCloseableHttpClient()
        {
            HttpClientBuilder httpClientBuilder = HttpClients.custom();
            CredentialsProvider provider = null;

            Registry<ConnectionSocketFactory> reg = RegistryBuilder
                    .<ConnectionSocketFactory>create()
                    .register("http", new MyConnectionSocketFactory())
                    .register("https", new MyConnectionSocketFactory())
                    .build();
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(reg);
            httpClientBuilder.setConnectionManager(cm);
            if (this.password != null) {
                httpClientBuilder = HttpClientBuilder.create();
                // setup BasicAuth
                provider = new BasicCredentialsProvider();
                // Create the authentication scope
                HttpHost target = new HttpHost(uriBuilder.getHost(), uriBuilder.getPort());
                AuthScope scope = new AuthScope(target);
                // Create credential pair
                UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(this.username, this.password);
                // Inject the credentials
                provider.setCredentials(scope, credentials);
                // Set the default credentials provider
            }

            if (this.proxyAuth != null) {
                String[] up = this.proxyAuth.split(":");
                System.setProperty("java.net.socks.username", up[0]);
                System.setProperty("http.proxyUser", up[0]);
                if (up.length == 2) {
                    System.setProperty("java.net.socks.password", up[1]);
                    System.setProperty("http.proxyPassword", up[1]);
                }
            }

            httpClientBuilder.setSSLSocketFactory(ignoreSSLErrors());

            return httpClientBuilder.build();
        }

        private SSLConnectionSocketFactory ignoreSSLErrors()
        {
            try {
                // use the TrustSelfSignedStrategy to allow Self Signed Certificates
                SSLContext sslContext = SSLContextBuilder
                        .create()
                        .loadTrustMaterial(new TrustSelfSignedStrategy())
                        .build();

                // we can optionally disable hostname verification.
                // if you don't want to further weaken the security, you don't have to include this.
                HostnameVerifier allowAllHosts = new NoopHostnameVerifier();

                // create an SSL Socket Factory to use the SSLContext with the trust self signed certificate strategy
                // and allow all hosts verifier.
                return new SSLConnectionSocketFactory(sslContext, allowAllHosts);
            }
            catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    static class MyConnectionSocketFactory
            implements
            ConnectionSocketFactory
    {

        @Override
        public Socket createSocket(final HttpContext context)
                throws IOException
        {
            Proxy proxy = null;
            URI uri = (URI) context.getAttribute("proxy");
            if (uri == null) {
                return null;
            }

            InetSocketAddress socksaddr = new InetSocketAddress(uri.getHost(), uri.getPort());
            String proxyType = uri.getScheme();
            if (proxyType.startsWith("socks")) {
                proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
            }
            else if (proxyType.startsWith("http")) {
                proxy = new Proxy(Proxy.Type.HTTP, socksaddr);
            }
            if (proxy == null) {
                return null;
            } else {
                return new Socket(proxy);
            }
        }

        @Override
        public Socket connectSocket(final int connectTimeout,
                final Socket socket, final HttpHost host,
                final InetSocketAddress remoteAddress,
                final InetSocketAddress localAddress,
                final HttpContext context)
                throws IOException
        {
            Socket sock;
            if (socket != null) {
                sock = socket;
            }
            else {
                sock = createSocket(context);
            }
            if (localAddress != null) {
                sock.bind(localAddress);
            }
            try {
                sock.connect(remoteAddress, connectTimeout);
            }
            catch (SocketTimeoutException ex) {
                throw new ConnectTimeoutException(ex, host,
                        remoteAddress.getAddress());
            }
            return sock;
        }
    }
}
