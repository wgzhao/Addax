package com.wgzhao.datax.core.hook;

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

/**
 * 将统计上报到指定的接口上
 * 接口从 conf/core.json 文件中的 $.core.dataXServer.address 中获取
 * 如果该配置项为空，则跳过该接口，否则，将通过该接口上传本次任务执行的统计信息
 * 详细信息可以参考文档 statsreport.md
 */
public class JobReport
{

    private static CloseableHttpAsyncClient client = null;

    private JobReport() {}

    public static CloseableHttpAsyncClient getHttpClient()
    {
        if (client == null) {
            synchronized (JobReport.class) {
                if (client == null) {
                    RequestConfig requestConfig = RequestConfig.custom()
                            .setConnectTimeout(2000)//连接超时,连接建立时间,三次握手完成时间
                            .setSocketTimeout(2000)//请求超时,数据传输过程中数据包之间间隔的最大时间
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
                        e.printStackTrace();
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
