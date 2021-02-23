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

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordReceiver;
import com.wgzhao.datax.common.spi.Writer;
import com.wgzhao.datax.common.util.Configuration;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;

        private String endpoint;
        private String username;
        private String password;
        private String database;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            List<Object> connList = originalConfig.getList(Key.CONNECTION);
            Configuration conn = Configuration.from(connList.get(0).toString());
            conn.getNecessaryValue(Key.TABLE, InfluxDBWriterErrorCode.REQUIRED_VALUE);
            this.endpoint = conn.getNecessaryValue(Key.ENDPOINT, InfluxDBWriterErrorCode.REQUIRED_VALUE);
            this.database = conn.getNecessaryValue(Key.DATABASE, InfluxDBWriterErrorCode.REQUIRED_VALUE);
            this.username = originalConfig.getString(Key.USERNAME);
            this.password = originalConfig.getString(Key.PASSWORD);
            List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        InfluxDBWriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.COLUMN + "] is not set.");
            }
        }

        @Override
        public void prepare()
        {
            List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
            if (!preSqls.isEmpty()) {
                try (InfluxDB influxDB = InfluxDBFactory.connect(this.endpoint, this.username, this.password)) {
                    influxDB.setDatabase(database);
                    for (String sql : preSqls) {
                        influxDB.query(new Query(sql));
                    }
                }
                catch (Exception e) {
                    throw DataXException.asDataXException(
                            InfluxDBWriterErrorCode.CONNECT_ERROR, e
                    );
                }
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            List<Configuration> splittedConfigs = new ArrayList<>();
            splittedConfigs.add(readerSliceConfig);
            return splittedConfigs;
        }

        @Override
        public void post()
        {
            List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
            if (!postSqls.isEmpty()) {
                try (InfluxDB influxDB = InfluxDBFactory.connect(endpoint, username, password)) {
                    influxDB.setDatabase(database);
                    for (String sql : postSqls) {
                        influxDB.query(new Query(sql));
                    }
                }
                catch (Exception e) {
                    throw DataXException.asDataXException(
                            InfluxDBWriterErrorCode.ILLEGAL_VALUE, e
                    );
                }
            }
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
        private InfluxDBWriterTask influxDBWriterTask;

        @Override
        public void init()
        {
            Configuration writerSliceConfig = super.getPluginJobConf();
            this.influxDBWriterTask = new InfluxDBWriterTask(writerSliceConfig);
            this.influxDBWriterTask.init();
        }

        @Override
        public void prepare()
        {
            this.influxDBWriterTask.prepare();
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.influxDBWriterTask.startWrite(recordReceiver, super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.influxDBWriterTask.post();
        }

        @Override
        public void destroy()
        {
            this.influxDBWriterTask.destroy();
        }
    }
}
