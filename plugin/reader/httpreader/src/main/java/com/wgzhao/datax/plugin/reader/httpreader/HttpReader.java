package com.wgzhao.datax.plugin.reader.httpreader;

import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.spi.Reader;
import com.wgzhao.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.HostnameVerifier;
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
        private HttpHost proxy;
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
                    jsonArray = JSON.parseArray(object.toString());
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
                    throw DataXException.asDataXException(
                            HttpReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set."
                    );
                }
                Record record;
                JSONObject jsonObject = jsonArray.getJSONObject(0);
                if (columns.size() == 1 && "*".equals(columns.get(0))) {
                    // 没有给定key的情况下，提取JSON的第一层key作为字段处理
                    columns.remove(0);
                    for (Object obj: JSONPath.keySet(jsonObject, "/")) {
                        columns.add(obj.toString());
                    }
                }
                // first, check key exists or not ?
                for (String k : columns) {
                    if (!JSONPath.contains(jsonObject, k)) {
                        throw DataXException.asDataXException(
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
                throw DataXException.asDataXException(
                        HttpReaderErrorCode.ILLEGAL_VALUE, e.getMessage()
                );
            }
        }

        private void createProxy(Configuration proxyConf)
        {
            String host = proxyConf.getString(Key.HOST);
            this.proxyAuth = proxyConf.getString(Key.AUTH);
            if (host.startsWith("socks")) {
                throw DataXException.asDataXException(
                        HttpReaderErrorCode.NOT_SUPPORT, "sockes 代理暂时不支持"
                );
            }
            URI uri = URI.create(host);
            this.proxy = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
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

                RequestConfig config = RequestConfig.custom()
                        .setProxy(proxy)
                        .build();
                request.setConfig(config);
                response = httpClient.execute(request);
            }
            else if ("post".equalsIgnoreCase(method)) {
                HttpPost request = new HttpPost(uriBuilder.build());
                headers.forEach((k, v) -> request.setHeader(k, v.toString()));
                httpClient = createCloseableHttpClient();
                RequestConfig config = RequestConfig.custom()
                        .setProxy(proxy)
                        .build();
                request.setConfig(config);
                response = httpClient.execute(request);
            }
            else {
                throw DataXException.asDataXException(
                        HttpReaderErrorCode.ILLEGAL_VALUE, "不支持的请求模式: " + method
                );
            }
            return response;
        }

        private CloseableHttpClient createCloseableHttpClient()
        {
            HttpClientBuilder httpClientBuilder = HttpClients.custom();
            CredentialsProvider provider = null;
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
                provider = new BasicCredentialsProvider();
                if (up.length == 1) {
                    provider.setCredentials(new AuthScope(this.proxy), new UsernamePasswordCredentials(up[0], null));
                }
                if (up.length == 2) {
                    provider.setCredentials(new AuthScope(this.proxy), new UsernamePasswordCredentials(up[0], up[1]));
                }
            }
            if (provider != null) {
                httpClientBuilder.setDefaultCredentialsProvider(provider);
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
}
