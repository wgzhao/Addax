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

package com.wgzhao.addax.plugin.reader.influxdb2reader;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.CONNECTION;
import static com.wgzhao.addax.common.base.Key.ENDPOINT;
import static com.wgzhao.addax.common.base.Key.QUERY_SQL;
import static com.wgzhao.addax.common.base.Key.TABLE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class InfluxDB2Reader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig = null;
        private String endpoint;
        private List<String> tables;
        private String org;
        private String bucket;
        private String token;
        private List<String> columns;
        private List<String> range;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare()
        {
            this.token = originalConfig.getNecessaryValue(InfluxDB2Key.TOKEN, REQUIRED_VALUE);
            originalConfig.getNecessaryValue(InfluxDB2Key.RANGE, REQUIRED_VALUE);
            this.range = originalConfig.getList(InfluxDB2Key.RANGE, String.class);
            Configuration connConf = Configuration.from(originalConfig.getList(CONNECTION, Object.class).get(0).toString());
            this.endpoint = connConf.getNecessaryValue(InfluxDB2Key.ENDPOINT, REQUIRED_VALUE);
            this.bucket = connConf.getNecessaryValue(InfluxDB2Key.BUCKET, REQUIRED_VALUE);
            this.org = connConf.getNecessaryValue(InfluxDB2Key.ORG, REQUIRED_VALUE);
            this.tables = connConf.getList(TABLE, String.class);
            this.columns = originalConfig.getList(COLUMN, String.class);

            this.originalConfig = dealColumns();
        }

        public Configuration dealColumns()
        {
            Configuration conf = this.originalConfig;
            if (columns.size() == 1 && "*".equals(columns.get(0))) {
                columns.clear();
            }

            String querySql = generalQueryQL();
            // write query sql
            conf.set(QUERY_SQL, querySql);

            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(endpoint, token.toCharArray(), org, bucket);
            QueryApi queryApi = influxDBClient.getQueryApi();
            // ONly get schema , so limit records to one
            final List<FluxTable> fluxTables = queryApi.query(querySql + " |> limit(n:1) ");
            if (fluxTables.isEmpty()) {
                return conf;
            }
            List<Map<String, String>> fluxColumns = new ArrayList<>();
            List<FluxColumn> allColumns = fluxTables.get(0).getColumns();
            List<String> labels = new ArrayList<>(allColumns.size());

            allColumns.forEach(k -> labels.add(k.getLabel()));

            if (columns.isEmpty()) {
                // skip internal fields;
                for (FluxColumn col : allColumns) {
                    String label = col.getLabel();
                    Map<String, String> map = new HashMap<>();
                    if ((label.startsWith("_") && !"_time".equals(label)) || "result".equals(label) || "table".equals(label)) {
                        continue;
                    }
                    map.put("name", label);
                    map.put("type", col.getDataType());
                    fluxColumns.add(map);
                }
            }
            else {
                for (String col : columns) {
                    if (labels.contains(col)) {
                        Map<String, String> map = new HashMap<>();
                        FluxColumn k = allColumns.get(labels.indexOf(col));
                        map.put("name", k.getLabel());
                        map.put("type", k.getDataType());
                        fluxColumns.add(map);
                    }
                    else {
                        throw AddaxException.asAddaxException(REQUIRED_VALUE,
                                "The column '" + col + "' you specified doest not exists");
                    }
                }
            }
            // write back columns info
            conf.set(COLUMN, fluxColumns);

            return conf;
        }

        private String generalQueryQL()
        {
            Configuration connConf = Configuration.from(originalConfig.getList(CONNECTION, Object.class).get(0).toString());
            String bucket = connConf.getString(InfluxDB2Key.BUCKET);
            String startTime = null, endTime = null;
            if (!range.isEmpty()) {
                startTime = range.get(0);
                if (range.size() == 2) {
                    endTime = range.get(1);
                }
            }

            StringBuilder queryBuilder = new StringBuilder();

            queryBuilder.append("from(bucket:\"").append(bucket).append("\")\n");
            if (startTime != null || endTime != null) {
                boolean hasStart = false;
                queryBuilder.append("  |> range(");
                if (startTime != null) {
                    queryBuilder.append("start: ").append(startTime);
                    hasStart = true;
                }
                if (endTime != null) {
                    if (hasStart) {
                        queryBuilder.append(", stop: ").append(endTime);
                    }
                    else {
                        queryBuilder.append("stop: ").append(endTime);
                    }
                }
                queryBuilder.append(") \n");
            }

            if (tables != null && !tables.isEmpty()) {
                queryBuilder.append("  |> filter(fn: (r) => ");
                queryBuilder.append(" r._measurement ==\"").append(tables.get(0));
                if (tables.size() > 1) {
                    for (int i = 1; i < tables.size(); i++) {
                        queryBuilder.append(" or r._measurement ==\"").append(tables.get(i));
                    }
                }
                queryBuilder.append("\") \n");
            }
            if (! columns.isEmpty()) {
                queryBuilder.append("  |> filter(fn: (r) => r._field ==\"").append(columns.get(0)).append("\" ");
                if (columns.size() > 1) {
                    for(int i=1; i<columns.size();i++) {
                        queryBuilder.append(" or r._field == \"").append(columns.get(i)).append("\" ");
                    }
                }
                queryBuilder.append(") \n");
            }
            // convert fields to columns
            // refers https://docs.influxdata.com/influxdb/v2.0/query-data/flux/calculate-percentages/
            queryBuilder.append("  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n");

            return queryBuilder.toString();
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            List<Configuration> splitConfigs = new ArrayList<>();
            splitConfigs.add(readerSliceConfig);
            return splitConfigs;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private String endpoint;
        private String token;
        private String org;
        private String bucket;
        private String queryQL;

        private List<Map> columns;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            Configuration connConf = Configuration.from(readerSliceConfig.getList(CONNECTION, Object.class).get(0).toString());
            this.endpoint = connConf.getString(ENDPOINT);
            this.token = readerSliceConfig.getString("token");
            this.org = connConf.getString("org");
            this.bucket = connConf.getString("bucket");
            this.columns = readerSliceConfig.getList(COLUMN, Map.class);
            this.queryQL = readerSliceConfig.getString(QUERY_SQL);
            if (readerSliceConfig.getInt(InfluxDB2Key.LIMIT) != null) {
                this.queryQL = this.queryQL + "  |> limit(n: " + readerSliceConfig.getInt(InfluxDB2Key.LIMIT) + " )";
            }
            LOG.info("query sql: \n{}", queryQL);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(endpoint, token.toCharArray(), org, bucket);

            QueryApi queryApi = influxDBClient.getQueryApi();

            List<FluxTable> tables = queryApi.query(queryQL);
            if (tables.isEmpty()) {
                influxDBClient.close();
                return;
            }

            for (FluxTable fluxTable : tables) {
                List<FluxRecord> records = fluxTable.getRecords();
                for (FluxRecord fluxRecord : records) {
                    Record record = recordSender.createRecord();

                    for (Map<String, String> column : columns) {
                        Object v = fluxRecord.getValueByKey(column.get("name"));
                        if (v == null) {
                            record.addColumn(new StringColumn());
                            continue;
                        }

                        switch (column.get("type")) {
                            case "long":
                            case "int":
                                record.addColumn(new LongColumn((long) v));
                                break;

                            case "double":
                            case "float":
                                record.addColumn(new DoubleColumn((double) v));
                                break;

                            case "string":
                            default:
                                record.addColumn(new StringColumn(v.toString()));
                                break;
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            }
            influxDBClient.close();
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
