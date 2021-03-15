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

package com.wgzhao.datax.plugin.writer.influxdbwriter;

import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordReceiver;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.util.Configuration;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBWriterTask
{
    private static final int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
    private static final int READ_TIMEOUT_SECONDS_DEFAULT = 20;
    private static final int WRITE_TIMEOUT_SECONDS_DEFAULT = 20;
    protected List<Configuration> columns;
    private final int columnNumber;
    private final int batchSize;
    private InfluxDB influxDB;

    private final List<String> postSqls;
    private final String table;
    private final String database;
    private final String endpoint;
    private final String username;
    private final String password;

    private final int connTimeout;
    private final int readTimeout;
    private final int writeTimeout;

    public InfluxDBWriterTask(Configuration configuration)
    {
        List<Object> connList = configuration.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        this.endpoint = conn.getString(Key.ENDPOINT);
        this.table = conn.getString(Key.TABLE);
        this.database = conn.getString(Key.DATABASE);
        this.columns = configuration.getListConfiguration(Key.COLUMN);
        this.columnNumber = this.columns.size();
        this.username = configuration.getString(Key.USERNAME);
        this.password = configuration.getString(Key.PASSWORD, null);
        this.connTimeout = configuration.getInt(Key.CONNECT_TIMEOUT_SECONDS, CONNECT_TIMEOUT_SECONDS_DEFAULT);
        this.readTimeout = configuration.getInt(Key.READ_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS_DEFAULT);
        this.writeTimeout = configuration.getInt(Key.WRITE_TIMEOUT_SECONDS, WRITE_TIMEOUT_SECONDS_DEFAULT);
        this.batchSize = configuration.getInt(Key.BATCH_SIZE, 1024);
        this.postSqls = configuration.getList(Key.POST_SQL, String.class);

    }

    public void init()
    {
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder()
                .connectTimeout(connTimeout, TimeUnit.SECONDS)
                .readTimeout(readTimeout, TimeUnit.SECONDS)
                .writeTimeout(writeTimeout, TimeUnit.SECONDS);

        this.influxDB = InfluxDBFactory.connect(endpoint, username, password, okHttpClientBuilder);
        this.influxDB.enableBatch(this.batchSize, this.writeTimeout, TimeUnit.SECONDS);
        influxDB.setDatabase(database);
    }

    public void prepare()
    {
        //
    }

    public void post()
    {

        if (!postSqls.isEmpty()) {
            for (String sql : postSqls) {
                this.influxDB.query(new Query(sql));
            }
        }
    }

    public void destroy()
    {
        //
    }

    public void startWrite(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector)
    {
        // Retention policy
        String rp = "autogen";
        Record record = null;
        try {
            while ((record = recordReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != this.columnNumber) {
                    throw DataXException.asDataXException(
                            InfluxDBWriterErrorCode.CONF_ERROR,
                            String.format(
                                    "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                    record.getColumnNumber(),
                                    this.columnNumber)
                    );
                }
                Point.Builder builder = Point.measurement(table);
                Map<String, Object> fields = new HashMap<>();
                // 第一列必须是时间戳类型，这是时序数据库的特征
                builder.time(record.getColumn(0).asLong(), TimeUnit.MILLISECONDS);
                for (int i = 1; i < columnNumber; i++) {
                    String name = this.columns.get(i).getString("name");
                    String type = this.columns.get(i).getString("type").toUpperCase();
                    Column column = record.getColumn(i);
                    switch (type) {
                        case "INT":
                        case "LONG":
                            fields.put(name, column.asLong());
                            break;
                        case "DATE":
                            fields.put(name, column.asDate());
                            break;
                        case "DOUBLE":
                            fields.put(name, column.asDouble());
                            break;
                        case "DECIMAL":
                            fields.put(name, column.asBigDecimal());
                            break;
                        case "BINARY":
                            fields.put(name, column.asBytes());
                            break;
                        case "TAG":
                            fields.put(name, column.asString());
                            break;
                        default:
                            fields.put(name, column.asString());
                            break;
                    }
                }
                builder.fields(fields);
                influxDB.write(database, rp, builder.build());
            }
        }
        catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            throw DataXException.asDataXException(InfluxDBWriterErrorCode.ILLEGAL_VALUE, e);
        }
    }
}
