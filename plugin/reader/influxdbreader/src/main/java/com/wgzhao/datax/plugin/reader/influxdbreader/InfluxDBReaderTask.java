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

import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.util.Configuration;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDBReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static final int READ_TIMEOUT_SECONDS_DEFAULT = 20;
    private static final int WRITE_TIMEOUT_SECONDS_DEFAULT = 20;

    private String querySql;
    private final String table;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;
    private final String where;

    private final int connTimeout;
    private final int readTimeout;
    private final int writeTimeout;

    public InfluxDBReaderTask(Configuration configuration)
    {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        this.querySql = configuration.getString(Key.QUERY_SQL, null);
        this.table = conn.getString(Key.TABLE);
        this.database = conn.getString(Key.DATABASE);
        this.endpoint = conn.getString(Key.ENDPOINT);
        this.username = configuration.getString(Key.USERNAME);
        this.password = configuration.getString(Key.PASSWORD, null);
        this.where = configuration.getString(Key.WHERE, null);
        this.connTimeout = configuration.getInt(Key.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT);
        this.readTimeout = configuration.getInt(Key.READ_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS_DEFAULT);
        this.writeTimeout = configuration.getInt(Key.WRITE_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS_DEFAULT);
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
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder()
                .connectTimeout(connTimeout, TimeUnit.SECONDS)
                .readTimeout(readTimeout, TimeUnit.SECONDS)
                .writeTimeout(writeTimeout, TimeUnit.SECONDS);
        InfluxDB influxDB = InfluxDBFactory.connect(endpoint, username, password, okHttpClientBuilder);
        influxDB.setDatabase(database);
        if (querySql == null) {
            if (where != null) {
                querySql = "select * from " + table + " where " + where;
            }
            else {
                querySql = "select * from " + table;
            }
        }

        QueryResult rs;
        Record record = null;
        try {
            rs = influxDB.query(new Query(querySql));
            influxDB.enableBatch();
            QueryResult.Result result = rs.getResults().get(0);
            List<QueryResult.Series> seriesList = result.getSeries();
            if (seriesList == null) {
                return;
            }
            QueryResult.Series series = seriesList.get(0);
            List<List<Object>> values = series.getValues();
            for (List<Object> row : values) {
                record = recordSender.createRecord();
                for (Object v : row) {
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
        catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            throw DataXException.asDataXException(InfluxDBReaderErrorCode.ILLEGAL_VALUE, e);
        }
        finally {
            influxDB.close();
        }
    }
}
