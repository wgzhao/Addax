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

package com.wgzhao.addax.plugin.reader.cassandrareader;

import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.wgzhao.addax.common.element.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraReader
        extends Reader
{
    private static final Logger LOG = LoggerFactory
            .getLogger(CassandraReader.class);

    public static class Job
            extends Reader.Job
    {

        private Configuration jobConfig = null;
        private Cluster cluster = null;

        @Override
        public void init()
        {
            this.jobConfig = super.getPluginJobConf();
            this.jobConfig = super.getPluginJobConf();
            String username = jobConfig.getString(MyKey.USERNAME);
            String password = jobConfig.getString(MyKey.PASSWORD);
            String hosts = jobConfig.getString(MyKey.HOST);
            Integer port = jobConfig.getInt(MyKey.PORT, 9042);
            boolean useSSL = jobConfig.getBool(MyKey.USE_SSL);

            if ((username != null) && !username.isEmpty()) {
                Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
                        .withPort(port).addContactPoints(hosts.split(","));
                if (useSSL) {
                    clusterBuilder = clusterBuilder.withSSL();
                }
                cluster = clusterBuilder.build();
            }
            else {
                cluster = Cluster.builder().withPort(port)
                        .addContactPoints(hosts.split(",")).build();
            }
            CassandraReaderHelper.checkConfig(jobConfig, cluster);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return CassandraReaderHelper.splitJob(adviceNumber, jobConfig, cluster);
        }
    }

    public static class Task
            extends Reader.Task
    {
        private Session session = null;
        private String queryString = null;
        private ConsistencyLevel consistencyLevel;
        private int columnNumber = 0;

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            String username = taskConfig.getString(MyKey.USERNAME);
            String password = taskConfig.getString(MyKey.PASSWORD);
            String hosts = taskConfig.getString(MyKey.HOST);
            Integer port = taskConfig.getInt(MyKey.PORT);
            boolean useSSL = taskConfig.getBool(MyKey.USE_SSL);
            String keyspace = taskConfig.getString(MyKey.KEYSPACE);
            List<String> columnMeta = taskConfig.getList(MyKey.COLUMN, String.class);
            columnNumber = columnMeta.size();

            Cluster cluster;
            if ((username != null) && !username.isEmpty()) {
                Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
                        .withPort(port).addContactPoints(hosts.split(","));
                if (useSSL) {
                    clusterBuilder = clusterBuilder.withSSL();
                }
                cluster = clusterBuilder.build();
            }
            else {
                cluster = Cluster.builder().withPort(port)
                        .addContactPoints(hosts.split(",")).build();
            }
            session = cluster.connect(keyspace);
            String cl = taskConfig.getString(MyKey.CONSISTENCY_LEVEL);
            if (cl != null && !cl.isEmpty()) {
                consistencyLevel = ConsistencyLevel.valueOf(cl);
            }
            else {
                consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
            }

            queryString = CassandraReaderHelper.getQueryString(taskConfig, cluster);
            LOG.info("query = " + queryString);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            ResultSet r = session.execute(new SimpleStatement(queryString).setConsistencyLevel(consistencyLevel));
            for (Row row : r) {
                Record record = recordSender.createRecord();
                record = CassandraReaderHelper.buildRecord(record, row, r.getColumnDefinitions(), columnNumber,
                        super.getTaskPluginCollector());
                if (record != null) {
                    recordSender.sendToWriter(record);
                }
            }
        }

        @Override
        public void destroy()
        {

        }
    }
}
