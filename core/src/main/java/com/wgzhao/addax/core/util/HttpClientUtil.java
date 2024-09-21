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

package com.wgzhao.addax.core.util;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.RetryUtil;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import static com.wgzhao.addax.common.exception.CommonErrorCode.RUNTIME_ERROR;

public class HttpClientUtil
{

    private static final int POOL_SIZE = 20;
    private static final ThreadPoolExecutor asyncExecutor = RetryUtil.createThreadPoolExecutor();
    private static CredentialsProvider provider;
    private static HttpClientUtil clientUtil;
    //构建httpclient的时候一定要设置这两个参数。淘宝很多生产故障都由此引起
    private static int HTTP_TIMEOUT_MILLISECONDS = 5000;
    private CloseableHttpClient httpClient;

    public HttpClientUtil()
    {
        initApacheHttpClient();
    }

    public static void setHttpTimeoutInMillionSeconds(int httpTimeoutInMillionSeconds)
    {
        HTTP_TIMEOUT_MILLISECONDS = httpTimeoutInMillionSeconds;
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
        RequestConfig requestConfig = RequestConfig
                .custom()
                .setSocketTimeout(HTTP_TIMEOUT_MILLISECONDS)
                .setConnectTimeout(HTTP_TIMEOUT_MILLISECONDS)
                .setConnectionRequestTimeout(HTTP_TIMEOUT_MILLISECONDS)
                .build();

        if (null == provider) {
            httpClient = HttpClientBuilder
                    .create()
                    .setMaxConnTotal(POOL_SIZE)
                    .setMaxConnPerRoute(POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).build();
        }
        else {
            httpClient = HttpClientBuilder
                    .create()
                    .setMaxConnTotal(POOL_SIZE)
                    .setMaxConnPerRoute(POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultCredentialsProvider(provider)
                    .build();
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
        String entityStr;
        response = httpClient.execute(httpRequestBase);

        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            System.err.println("The request address：" + httpRequestBase.getURI()
                    + ", The request method：" + httpRequestBase.getMethod()
                    + ",STATUS CODE = " + response.getStatusLine().getStatusCode());
            httpRequestBase.abort();
            throw new Exception("Response Status Code : "
                    + response.getStatusLine().getStatusCode());
        }
        else {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                entityStr = EntityUtils.toString(entity, Consts.UTF_8);
            }
            else {
                throw new Exception("Response Entity Is Null");
            }
        }

        return entityStr;
    }

    public String executeAndGetWithFailedRetry(HttpRequestBase httpRequestBase,
            int retryTimes, long retryInterval)
    {
        try {
            return RetryUtil.asyncExecuteWithRetry(() -> {
                String result = executeAndGet(httpRequestBase);
                if (result != null && result.startsWith("{\"result\":-1")) {
                    throw AddaxException.asAddaxException(
                            RUNTIME_ERROR, "The return code is -1, try again.");
                }
                return result;
            }, retryTimes, retryInterval, true,
                    HTTP_TIMEOUT_MILLISECONDS + 1000, asyncExecutor);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }
}
