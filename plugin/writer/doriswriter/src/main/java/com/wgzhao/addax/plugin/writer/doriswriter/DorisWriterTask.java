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

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class DorisWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterTask.class);
    private final Configuration configuration;
    private String username;
    private String password;
    private String loadUrl;
    private List<String> column;
    private static final String SEPARATOR = "|";
    private int batchSize;
    private HttpClientBuilder httpClientBuilder;

    public DorisWriterTask(Configuration configuration) {this.configuration = configuration;}

    public void init()
    {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        String endpoint = conn.getString(Key.ENDPOINT);
        String table = conn.getString(Key.TABLE);
        String database = conn.getString(Key.DATABASE);
        this.column = configuration.getList(Key.COLUMN, String.class);
        // 如果 column 填写的是 * ，直接设置为null，方便后续判断
        if (this.column!= null && this.column.size() == 1 && "*".equals(this.column.get(0))) {
            this.column = null;
        }
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, 1024);
        this.username = configuration.getString(Key.USERNAME);
        this.password = configuration.getString(Key.PASSWORD, null);
        this.loadUrl = String.format("%s/api/%s/%s/_stream_load", endpoint, database, table);
        this.httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy()
                {
                    @Override
                    protected boolean isRedirectable(String method)
                    {
                        return true;
                    }
                });
        LOG.info("connect DorisDB with {}", this.loadUrl);
    }

    private String basicAuthHeader(String username, String password)
    {
        String tokenEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tokenEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public void startWrite(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector)
    {
        Record record;
        int currSize = 0;
        StringBuilder stringBuilder = new StringBuilder();
        while ((record = recordReceiver.getFromReader()) != null) {
            int len = record.getColumnNumber();
            if (this.column != null && len != this.column.size()) {
                throw AddaxException.asAddaxException(
                        DorisWriterErrorCode.ILLEGAL_VALUE,
                        String.format("源字段数和目标字段数不匹配，源字段数为%s, 目标字段数为%s", len, this.column.size())
                );
            }
            StringBuilder oneRow = new StringBuilder();
            for (int i = 0; i < len; i++) {
                if (record.getColumn(i).getRawData() != null) {
                    oneRow.append(record.getColumn(i).asString());
                }
                if (i < len - 1) {
                    oneRow.append(SEPARATOR);
                }
            }
            oneRow.append("\n");
            stringBuilder.append(oneRow);
            currSize++;
            if (currSize >= this.batchSize) {
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                sendData(stringBuilder.toString());
                currSize = 0;
                stringBuilder.setLength(0);
            }
        }
        if (stringBuilder.length() > 0) {
            sendData(stringBuilder.toString());
        }
    }

    private void sendData(String content)
    {
        try (CloseableHttpClient client = this.httpClientBuilder.build()) {
            HttpPut put = new HttpPut(this.loadUrl);
            StringEntity entity = new StringEntity(content, "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            if (this.username != null && this.password != null) {
                put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(this.username, this.password));
            }
            put.setHeader("column_separator", SEPARATOR);
            // the label header is optional, not necessary
            // use label header can ensure at most once semantics
            put.setHeader("label", UUID.randomUUID().toString());
            if (this.column != null ) {
                put.setHeader("columns", String.join(",", this.column));
            }
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that doris be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200) {
                    throw AddaxException.asAddaxException(
                            DorisWriterErrorCode.WRITER_ERROR,
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult)
                    );
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    DorisWriterErrorCode.CONNECT_ERROR,
                    String.format("Failed to connect Doris server with: %s, %s", this.loadUrl, e)
            );
        }
    }
}
