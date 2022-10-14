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

package com.wgzhao.addax.core.hook;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将统计上报到指定的接口上
 * 接口从 conf/core.json 文件中的 $.core.addaXServer.address 中获取
 * 如果该配置项为空，则跳过该接口，否则，将通过该接口上传本次任务执行的统计信息
 * 详细信息可以参考文档 statsreport.md
 */
public class JobReport
{

    private static CloseableHttpAsyncClient client = null;

    private static final Logger logger = LoggerFactory.getLogger(JobReport.class);

    private JobReport() {}

    public static CloseableHttpAsyncClient getHttpClient(int timeout)
    {
        if (client == null) {
            synchronized (JobReport.class) {
                if (client == null) {
                    RequestConfig requestConfig = RequestConfig.custom()
                            .setConnectTimeout(timeout)//连接超时,连接建立时间,三次握手完成时间
                            .setSocketTimeout(timeout)//请求超时,数据传输过程中数据包之间间隔的最大时间
                            .setConnectionRequestTimeout(20000)//使用连接池来管理连接,从连接池获取连接的超时时间
                            .build();

                    //配置io线程
                    IOReactorConfig ioReactorConfig = IOReactorConfig.custom().
                            setIoThreadCount(Runtime.getRuntime().availableProcessors())
                            .setSoKeepAlive(true)
                            .build();
                    //设置连接池大小
                    ConnectingIOReactor ioReactor = null;
                    try {
                        ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
                    }
                    catch (IOReactorException e) {
                        logger.error(e.getMessage());
                    }
                    assert ioReactor != null;
                    PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
                    connManager.setMaxTotal(5);//最大连接数设置1
                    connManager.setDefaultMaxPerRoute(5);//per route最大连接数设置

                    client = HttpAsyncClients.custom()
                            .setConnectionManager(connManager)
                            .setDefaultRequestConfig(requestConfig)
                            .build();
                    client.start();
                }
            }
        }
        return client;
    }

    public static HttpPost getPostBody(String urls, String bodys, ContentType contentType)
    {
        HttpPost post;
        StringEntity entity;
        post = new HttpPost(urls);
        entity = new StringEntity(bodys, contentType);
        post.setEntity(entity);
        return post;
    }
}
