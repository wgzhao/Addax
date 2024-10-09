/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.reader.influxdbreader;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;

public class InfluxDBReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static final int SOCKET_TIMEOUT_SECONDS_DEFAULT = 20;

    private String querySql;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;

    private final int connTimeout;

    public InfluxDBReaderTask(Configuration configuration)
    {
        Configuration conn = configuration.getConfiguration(InfluxDBKey.CONNECTION);
        this.querySql = configuration.getString(InfluxDBKey.QUERY_SQL, null);
        this.database = conn.getString(InfluxDBKey.DATABASE);
        this.endpoint = conn.getString(InfluxDBKey.ENDPOINT);
        if (this.querySql == null) {
            this.querySql = "select * from " + conn.getString(InfluxDBKey.TABLE);
        }
        if (!"".equals(configuration.getString(InfluxDBKey.WHERE, ""))) {
            this.querySql += " where " + configuration.getString(InfluxDBKey.WHERE);
        }
        this.username = configuration.getString(InfluxDBKey.USERNAME, "");
        this.password = configuration.getString(InfluxDBKey.PASSWORD, "");
        this.connTimeout = configuration.getInt(InfluxDBKey.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT);
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
        String result;
        try {
            result = Request.get(combineUrl())
                    .connectTimeout(Timeout.ofSeconds(connTimeout))
                    .execute()
                    .returnContent().asString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (StringUtils.isBlank(result)) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Get nothing!", null);
        }
        try {
            JSONObject jsonObject = JSONObject.parseObject(result);
            JSONArray results = (JSONArray) jsonObject.get("results");
            JSONObject resultsMap = (JSONObject) results.get(0);
            if (resultsMap.containsKey("series")) {
                JSONArray series = (JSONArray) resultsMap.get("series");
                JSONObject seriesMap = (JSONObject) series.get(0);
                if (seriesMap.containsKey("values")) {
                    JSONArray values = (JSONArray) seriesMap.get("values");
                    for (Object row : values) {
                        JSONArray rowArray = (JSONArray) row;
                        Record record = recordSender.createRecord();
                        for (Object s : rowArray) {
                            if (null != s) {
                                record.addColumn(new StringColumn(s.toString()));
                            }
                            else {
                                record.addColumn(new StringColumn());
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                }
            }
            else if (resultsMap.containsKey("error")) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, "Error occurred in data sets！", null);
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Failed to send data", e);
        }
    }

    private String combineUrl()
    {
        String enc = "utf-8";
        try {
            String url = endpoint + "/query?db=" + URLEncoder.encode(database, enc);
            if (!"".equals(username)) {
                url += "&u=" + URLEncoder.encode(username, enc);
            }
            if (!"".equals(password)) {
                url += "&p=" + URLEncoder.encode(password, enc);
            }
            if (querySql.contains("#lastMinute#")) {
                this.querySql = querySql.replace("#lastMinute#", getLastMinute());
            }
            url += "&q=" + URLEncoder.encode(querySql, enc);
            return url;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "Failed to get data point！", e);
        }
    }

    @SuppressWarnings("JavaTimeDefaultTimeZone")
    private String getLastMinute()
    {
        long lastMinuteMilli = LocalDateTime.now().plusMinutes(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return String.valueOf(lastMinuteMilli);
    }
}
