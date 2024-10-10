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

package com.wgzhao.addax.plugin.writer.influxdb2writer;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.CONNECTION;
import static com.wgzhao.addax.common.base.Key.ENDPOINT;
import static com.wgzhao.addax.common.base.Key.TABLE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class InfluxDB2Writer
        extends Writer
{

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            originalConfig.getNecessaryValue(InfluxDB2Key.TOKEN, REQUIRED_VALUE);
            Configuration connConf = originalConfig.getConfiguration(CONNECTION);
            connConf.getNecessaryValue(InfluxDB2Key.ENDPOINT, REQUIRED_VALUE);
            connConf.getNecessaryValue(InfluxDB2Key.BUCKET, REQUIRED_VALUE);
            connConf.getNecessaryValue(InfluxDB2Key.ORG, REQUIRED_VALUE);
            connConf.getNecessaryValue(TABLE, REQUIRED_VALUE);
            List<String> columns = originalConfig.getList(COLUMN, String.class);
            if (columns == null || columns.isEmpty() || (columns.size() == 1 && "*".equals(columns.get(0)))) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The column must be configured and '*' is not supported yet");
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> splitConfigs = new ArrayList<>();
            splitConfigs.add(originalConfig);
            return splitConfigs;
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Writer.Task
    {
        private String endpoint;
        private String token;
        private String org;
        private String bucket;
        private String table;

        private List<String> columns;
        private List<Map> tags;
        private WritePrecision wp;
        private int batchSize;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            // get connection information
            Configuration connConf = readerSliceConfig.getConfiguration(CONNECTION);
            this.endpoint = connConf.getString(ENDPOINT);
            this.org = connConf.getString(InfluxDB2Key.ORG);
            this.bucket = connConf.getString(InfluxDB2Key.BUCKET);
            this.table = connConf.getString(TABLE);

            this.token = readerSliceConfig.getString(InfluxDB2Key.TOKEN);
            this.columns = readerSliceConfig.getList(COLUMN, String.class);
            this.tags = readerSliceConfig.getList(InfluxDB2Key.TAG, Map.class);
            this.wp = WritePrecision.valueOf(readerSliceConfig.getString(InfluxDB2Key.INTERVAL, "ms").toUpperCase());
            this.batchSize = readerSliceConfig.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            Record record;
            Column column;

            InfluxDBClient influxDBClient = InfluxDBClientFactory.create(endpoint, token.toCharArray(), org, bucket);
            WriteApi writeApi = influxDBClient.makeWriteApi();
            List<Point> points = new ArrayList<>(batchSize);

            while ((record = lineReceiver.getFromReader()) != null) {
                Point point = Point.measurement(table);
                if (!tags.isEmpty()) {
                    tags.forEach(point::addTags);
                }

                // The first column must be timestamp
                column = record.getColumn(0);
                final Instant instant = column.asTimestamp().toInstant();
                point.time(processTimeUnit(instant), wp);
                for (int i = 0; i < columns.size(); i++) {
                    String name = columns.get(i);
                    column = record.getColumn(i + 1); // the first field has processed above

                    switch (column.getType()) {
                        case LONG:
                            point.addField(name, column.asLong());
                            break;
                        case DOUBLE:
                            point.addField(name, column.asDouble());
                            break;
                        case BOOL:
                            point.addField(name, column.asBoolean());
                            break;
                        case DATE:
                        default:
                            point.addField(name, column.asString());
                            break;
                    }
                }
                points.add(point);
                if (points.size() == batchSize) {
                    writeApi.writePoints(points);
                    points.clear();
                }
            }
            // write remain points if present
            if (!points.isEmpty()) {
                writeApi.writePoints(points);
            }
            influxDBClient.close();
        }

        private long processTimeUnit(Instant instant)
        {
            long ts;
            switch (wp) {
                case S:
                    ts = instant.getEpochSecond();
                    break;
                case US:
                    ts = instant.getNano() / 1000;
                    break;
                case NS:
                    ts = instant.getNano();
                    break;
                default:
                    ts = instant.toEpochMilli();
                    break;
            }
            return ts;
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
