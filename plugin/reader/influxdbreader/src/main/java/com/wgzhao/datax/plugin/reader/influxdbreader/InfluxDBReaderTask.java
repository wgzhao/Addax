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
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InfluxDBReaderTask
{
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBReaderTask.class);

    private final Configuration configuration;

    public InfluxDBReaderTask(Configuration configuration) {this.configuration = configuration;}

    public void post()
    {
    }

    public void destroy()
    {
    }

    public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
            TaskPluginCollector taskPluginCollector)
    {
        List<Object> connList = readerSliceConfig.getList(Key.CONNECTION);
        Configuration conn = Configuration.from(connList.get(0).toString());
        String querySql = readerSliceConfig.getString(Key.QUERY_SQL, null);
        String table = conn.getString(Key.TABLE);
        String database = conn.getString(Key.DATABASE);
        String endpoint = conn.getString(Key.ENDPOINT);
        String username = readerSliceConfig.getString(Key.USERNAME);
        String password = readerSliceConfig.getString(Key.PASSWORD, null);
        String where = readerSliceConfig.getString(Key.WHERE, null);
        String basicMsg = String.format("http server:[%s]", endpoint);
        LOG.info("connect influxdb: {} with username: {}", endpoint, username);
        InfluxDB influxDB = InfluxDBFactory.connect(endpoint, username, password);
        influxDB.setDatabase(database);
        if (querySql == null) {
            if (where != null) {
                querySql = "select * from " + table + " where " + where;
            } else {
                querySql = "select * from " + table;
            }
        }

        QueryResult rs;
        Record record = null;
        try {
            rs = influxDB.query(new Query(querySql));
            QueryResult.Result  result  = rs.getResults().get(0);
            List<QueryResult.Series> seriesList = result.getSeries();
            if (seriesList == null) {
                return;
            }
            QueryResult.Series series = seriesList.get(0);
            List<List<Object>> values = series.getValues();
            for(List<Object> row : values){
                record = recordSender.createRecord();
                for(Object v : row) {
                    if (v == null) {
                        record.addColumn(new StringColumn(null));
                    } else {
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
