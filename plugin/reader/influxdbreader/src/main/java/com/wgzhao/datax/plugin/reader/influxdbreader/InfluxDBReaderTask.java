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

package com.wgzhao.datax.plugin.reader.influxdbreader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class InfluxDBReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static  final int SOCKET_TIMEOUT_SECONDS_DEFAULT = 20;

    private String querySql;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;

    private final int connTimeout;
    private final int socketTimeout;

    public InfluxDBReaderTask(Configuration configuration)
    {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        this.querySql = configuration.getString(Key.QUERY_SQL, null);
        this.database = conn.getString(Key.DATABASE);
        this.endpoint = conn.getString(Key.ENDPOINT);
        this.username = configuration.getString(Key.USERNAME);
        this.password = configuration.getString(Key.PASSWORD, null);
        this.connTimeout = configuration.getInt(Key.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT) * 1000;
        this.socketTimeout = configuration.getInt(Key.SOCKET_TIMEOUT_SECONDS, SOCKET_TIMEOUT_SECONDS_DEFAULT) * 1000;
    }

    public void post()
    {
        //
    }

    public void destroy()
    {
        //
    }

    public void startRead(RecordSender recordSender, TaskPluginCollector taskPluginCollector)
    {
        LOG.info("connect influxdb: {} with username: {}", endpoint, username);

        String tail="/query";
        String enc="utf-8";
        String result= "";
        try {
            String url=endpoint+tail
                    +"?db=" + URLEncoder.encode(database,enc) ;
            if(!"".equals(username)){
                url+="&u=" + URLEncoder.encode(username,enc);
            }
            if(!"".equals(password)){
                url+="&p=" + URLEncoder.encode(password,enc) ;
            }
            if(querySql.contains("#lastMinute#")){
                this.querySql = querySql.replace("#lastMinute#", getLastMinute());
            }
            url+="&q=" + URLEncoder.encode(querySql,enc);
            result = get(url);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Failed to get data point！", e);
        }

        if(StringUtils.isBlank(result)){
            throw DataXException.asDataXException(
                    InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Get nothing!", null);
        }
        try {
            JSONObject jsonObject = JSONObject.parseObject(result);
            JSONArray results = (JSONArray) jsonObject.get("results");
            JSONObject resultsMap = (JSONObject) results.get(0);
            if(resultsMap.containsKey("series")){
                JSONArray series= (JSONArray) resultsMap.get("series");
                JSONObject seriesMap = (JSONObject) series.get(0);
                if(seriesMap.containsKey("values")){
                    JSONArray values = (JSONArray) seriesMap.get("values");
                    for (Object row:values) {
                        JSONArray rowArray = (JSONArray) row;
                        Record record = recordSender.createRecord();
                        for (Object s:rowArray) {
                            if(null!=s){
                                record.addColumn(new StringColumn(s.toString()));
                            }else {
                                record.addColumn(new StringColumn(null));
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                }

            }else if(resultsMap.containsKey("error")){
                throw DataXException.asDataXException(
                        InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Error occured in data sets！", null);
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(
                    InfluxDBReaderErrorCode.ILLEGAL_VALUE, "Failed to send data", e);
        }
    }

    public String get(String url) throws Exception {
        Content content = Request.Get(url)
                .connectTimeout(this.connTimeout)
                .socketTimeout(this.socketTimeout)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(StandardCharsets.UTF_8);
    }

    private String post(String url, Map<String, Object> params) throws Exception {
        return post(url, JSON.toJSONString(params), this.connTimeout, this.socketTimeout);
    }

    private String post(String url, String params) throws Exception {
        return post(url, params, this.connTimeout , this.socketTimeout);
    }

    private String post(String url, Map<String, Object> params,
            int connectTimeoutInMill, int socketTimeoutInMill) throws Exception {
        return post(url, JSON.toJSONString(params), connectTimeoutInMill, socketTimeoutInMill);
    }

    private String post(String url, String params,
            int connectTimeoutInMill, int socketTimeoutInMill) throws Exception {
        Content content = Request.Post(url)
                .connectTimeout(connectTimeoutInMill)
                .socketTimeout(socketTimeoutInMill)
                .addHeader("Content-Type", "application/json")
                .bodyString(params, ContentType.APPLICATION_JSON)
                .execute()
                .returnContent();
        if (content == null) {
            return null;
        }
        return content.asString(StandardCharsets.UTF_8);
    }

    private String getLastMinute(){
        long lastMinuteMilli= LocalDateTime.now().plusMinutes(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return String.valueOf(lastMinuteMilli);
    }

}
