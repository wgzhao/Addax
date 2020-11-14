package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public class HttpClientUtil
{

    private static final int POOL_SIZE = 20;
    private static final ThreadPoolExecutor asyncExecutor = RetryUtil.createThreadPoolExecutor();
    private static CredentialsProvider provider;
    private static HttpClientUtil clientUtil;
    //构建httpclient的时候一定要设置这两个参数。淘宝很多生产故障都由此引起
    private static int HTTP_TIMEOUT_INMILLIONSECONDS = 5000;
    private CloseableHttpClient httpClient;

    public HttpClientUtil()
    {
        Properties prob = SecretUtil.getSecurityProperties();
        HttpClientUtil.setBasicAuth(prob.getProperty("auth.user"), prob.getProperty("auth.pass"));
        initApacheHttpClient();
    }

    public static void setHttpTimeoutInMillionSeconds(int httpTimeoutInMillionSeconds)
    {
        HTTP_TIMEOUT_INMILLIONSECONDS = httpTimeoutInMillionSeconds;
    }

    public static synchronized HttpClientUtil getHttpClientUtil()
    {
        if (null == clientUtil) {
            synchronized (HttpClientUtil.class) {
                if (null == clientUtil) {
                    clientUtil = new HttpClientUtil();
                }
            }
        }
        return clientUtil;
    }

    public static void setBasicAuth(String username, String password)
    {
        provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
    }

    public static HttpGet getGetRequest()
    {
        return new HttpGet();
    }

    public void destroy()
    {
        destroyApacheHttpClient();
    }

    // 创建包含connection pool与超时设置的client
    private void initApacheHttpClient()
    {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                .setConnectTimeout(HTTP_TIMEOUT_INMILLIONSECONDS).setConnectionRequestTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                .build();

        if (null == provider) {
            httpClient = HttpClientBuilder.create().setMaxConnTotal(POOL_SIZE).setMaxConnPerRoute(POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).build();
        }
        else {
            httpClient = HttpClientBuilder.create().setMaxConnTotal(POOL_SIZE).setMaxConnPerRoute(POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).setDefaultCredentialsProvider(provider).build();
        }
    }

    private void destroyApacheHttpClient()
    {
        try {
            if (httpClient != null) {
                httpClient.close();
                httpClient = null;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String executeAndGet(HttpRequestBase httpRequestBase)
            throws Exception
    {
        HttpResponse response;
        String entiStr;
        response = httpClient.execute(httpRequestBase);

        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            System.err.println("请求地址：" + httpRequestBase.getURI() + ", 请求方法：" + httpRequestBase.getMethod()
                    + ",STATUS CODE = " + response.getStatusLine().getStatusCode());
            httpRequestBase.abort();
            throw new Exception("Response Status Code : " + response.getStatusLine().getStatusCode());
        }
        else {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                entiStr = EntityUtils.toString(entity, Consts.UTF_8);
            }
            else {
                throw new Exception("Response Entity Is Null");
            }
        }

        return entiStr;
    }

    public String executeAndGetWithRetry(final HttpRequestBase httpRequestBase, final int retryTimes, final long retryInterval)
    {
        try {
            return RetryUtil.asyncExecuteWithRetry(() -> executeAndGet(httpRequestBase), retryTimes, retryInterval, true, HTTP_TIMEOUT_INMILLIONSECONDS + 1000, asyncExecutor);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    public String executeAndGetWithFailedRetry(final HttpRequestBase httpRequestBase, final int retryTimes, final long retryInterval)
    {
        try {
            return RetryUtil.asyncExecuteWithRetry(() -> {
                String result = executeAndGet(httpRequestBase);
                if (result != null && result.startsWith("{\"result\":-1")) {
                    throw DataXException.asDataXException(FrameworkErrorCode.CALL_REMOTE_FAILED, "远程接口返回-1,将重试");
                }
                return result;
            }, retryTimes, retryInterval, true, HTTP_TIMEOUT_INMILLIONSECONDS + 1000, asyncExecutor);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }
}
