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

package com.wgzhao.addax.plugin.writer.influxdbwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
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
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;

        private String endpoint;
        private String username;
        private String password;
        private String database;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            List<Object> connList = originalConfig.getList(InfluxDBKey.CONNECTION);
            Configuration conn = Configuration.from(connList.get(0).toString());
            conn.getNecessaryValue(InfluxDBKey.TABLE, InfluxDBWriterErrorCode.REQUIRED_VALUE);
            this.endpoint = conn.getNecessaryValue(InfluxDBKey.ENDPOINT, InfluxDBWriterErrorCode.REQUIRED_VALUE);
            this.database = conn.getNecessaryValue(InfluxDBKey.DATABASE, InfluxDBWriterErrorCode.REQUIRED_VALUE);
            this.username = originalConfig.getString(InfluxDBKey.USERNAME);
            this.password = originalConfig.getString(InfluxDBKey.PASSWORD);
            List<String> columns = originalConfig.getList(InfluxDBKey.COLUMN, String.class);
            if (columns == null || columns.isEmpty()) {
                throw AddaxException.asAddaxException(
                        InfluxDBWriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + InfluxDBKey.COLUMN + "] is not set.");
            }
        }

        @Override
        public void prepare()
        {
            List<String> preSqls = originalConfig.getList(InfluxDBKey.PRE_SQL, String.class);
            if (!preSqls.isEmpty()) {
                try (InfluxDB influxDB = InfluxDBFactory.connect(this.endpoint, this.username, this.password)) {
                    influxDB.setDatabase(database);
                    for (String sql : preSqls) {
                        influxDB.query(new Query(sql));
                    }
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            InfluxDBWriterErrorCode.CONNECT_ERROR, e
                    );
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(this.originalConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void post()
        {
            List<String> postSqls = originalConfig.getList(InfluxDBKey.POST_SQL, String.class);
            if (!postSqls.isEmpty()) {
                try (InfluxDB influxDB = InfluxDBFactory.connect(endpoint, username, password)) {
                    influxDB.setDatabase(database);
                    for (String sql : postSqls) {
                        influxDB.query(new Query(sql));
                    }
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
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
            Configuration writerSliceConfig = getPluginJobConf();
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
            this.influxDBWriterTask.startWrite(recordReceiver, getTaskPluginCollector());
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
