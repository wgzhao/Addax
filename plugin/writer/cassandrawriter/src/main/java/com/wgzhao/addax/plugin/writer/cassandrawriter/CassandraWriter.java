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

package com.wgzhao.addax.plugin.writer.cassandrawriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.EXECUTE_FAIL;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public class CassandraWriter
        extends Writer
{
    private static final Logger LOG = LoggerFactory
            .getLogger(CassandraWriter.class);

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originalConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void init()
        {
            originalConfig = getPluginJobConf();
        }

        @Override
        public void destroy()
        {

        }
    }

    public static class Task
            extends Writer.Task
    {
        private Session session = null;
        private PreparedStatement statement = null;
        private int columnNumber = 0;
        private List<DataType> columnTypes;
        private int writeTimeCol = -1;
        private boolean asyncWrite = false;
        private long batchSize = 1;
        private List<ResultSetFuture> unConfirmedWrite;
        private List<BoundStatement> bufferedWrite;

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            try {
                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw AddaxException
                                .asAddaxException(
                                        CONFIG_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    BoundStatement boundStmt = statement.bind();
                    for (int i = 0; i < columnNumber; i++) {
                        if (writeTimeCol != -1 && i == writeTimeCol) {
                            continue;
                        }
                        Column col = record.getColumn(i);
                        int pos = i;
                        if (writeTimeCol != -1 && pos > writeTimeCol) {
                            pos = i - 1;
                        }
                        CassandraWriterHelper.setupColumn(boundStmt, pos, columnTypes.get(pos), col);
                    }
                    if (writeTimeCol != -1) {
                        Column col = record.getColumn(writeTimeCol);
                        boundStmt.setLong(columnNumber - 1, col.asLong());
                    }
                    if (batchSize <= 1) {
                        session.execute(boundStmt);
                    }
                    else {
                        if (asyncWrite) {
                            unConfirmedWrite.add(session.executeAsync(boundStmt));
                            if (unConfirmedWrite.size() >= batchSize) {
                                for (ResultSetFuture write : unConfirmedWrite) {
                                    write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                                }
                                unConfirmedWrite.clear();
                            }
                        }
                        else {
                            bufferedWrite.add(boundStmt);
                            if (bufferedWrite.size() >= batchSize) {
                                BatchStatement batchStatement = new BatchStatement(Type.UNLOGGED);
                                batchStatement.addAll(bufferedWrite);
                                try {
                                    session.execute(batchStatement);
                                }
                                catch (Exception e) {
                                    LOG.error("batch写入失败，尝试逐条写入.", e);
                                    for (BoundStatement stmt : bufferedWrite) {
                                        session.execute(stmt);
                                    }
                                }
                                ///LOG.info("batch finished. size = " + bufferedWrite.size());
                                bufferedWrite.clear();
                            }
                        }
                    }
                }
                if (unConfirmedWrite != null && unConfirmedWrite.size() > 0) {
                    for (ResultSetFuture write : unConfirmedWrite) {
                        write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                    }
                    unConfirmedWrite.clear();
                }
                if (bufferedWrite != null && bufferedWrite.size() > 0) {
                    BatchStatement batchStatement = new BatchStatement(Type.UNLOGGED);
                    batchStatement.addAll(bufferedWrite);
                    session.execute(batchStatement);
                    bufferedWrite.clear();
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        EXECUTE_FAIL, e);
            }
        }

        @Override
        public void init()
        {
            Configuration taskConfig = super.getPluginJobConf();
            String username = taskConfig.getString(CassandraKey.USERNAME);
            String password = taskConfig.getString(CassandraKey.PASSWORD);
            String hosts = taskConfig.getString(CassandraKey.HOST);
            Integer port = taskConfig.getInt(CassandraKey.PORT, 9042);
            boolean useSSL = taskConfig.getBool(CassandraKey.USE_SSL);
            String keyspace = taskConfig.getString(CassandraKey.KEYSPACE);
            String table = taskConfig.getString(CassandraKey.TABLE);
            batchSize = taskConfig.getLong(CassandraKey.BATCH_SIZE, 1);
            List<String> columnMeta = taskConfig.getList(CassandraKey.COLUMN, String.class);
            columnTypes = new ArrayList<>(columnMeta.size());
            columnNumber = columnMeta.size();
            asyncWrite = taskConfig.getBool(CassandraKey.ASYNC_WRITE, false);

            int connectionsPerHost = taskConfig.getInt(CassandraKey.CONNECTIONS_PER_HOST, 8);
            int maxPendingPerConnection = taskConfig.getInt(CassandraKey.MAX_PENDING_CONNECTION, 128);
            PoolingOptions poolingOpts = new PoolingOptions()
                    .setConnectionsPerHost(HostDistance.LOCAL, connectionsPerHost, connectionsPerHost)
                    .setMaxRequestsPerConnection(HostDistance.LOCAL, maxPendingPerConnection)
                    .setNewConnectionThreshold(HostDistance.LOCAL, 100);
            Cluster.Builder clusterBuilder = Cluster.builder().withPoolingOptions(poolingOpts);
            if ((username != null) && !username.isEmpty()) {
                clusterBuilder = clusterBuilder.withCredentials(username, password)
                        .withPort(port).addContactPoints(hosts.split(","));
                if (useSSL) {
                    clusterBuilder = clusterBuilder.withSSL();
                }
            }
            else {
                clusterBuilder = clusterBuilder.withPort(port)
                        .addContactPoints(hosts.split(","));
            }
            Cluster cluster = clusterBuilder.build();
            session = cluster.connect(keyspace);
            TableMetadata meta = cluster.getMetadata().getKeyspace(keyspace).getTable(table);

            Insert insertStmt = QueryBuilder.insertInto(table);
            for (String colunmnName : columnMeta) {
                if (colunmnName.toLowerCase().equals(CassandraKey.WRITE_TIME)) {
                    if (writeTimeCol != -1) {
                        throw AddaxException
                                .asAddaxException(
                                        CONFIG_ERROR,
                                        "列配置信息有错误. 只能有一个时间戳列(writetime())");
                    }
                    writeTimeCol = columnTypes.size();
                    continue;
                }
                insertStmt.value(colunmnName, QueryBuilder.bindMarker());
                ColumnMetadata col = meta.getColumn(colunmnName);
                if (col == null) {
                    throw AddaxException
                            .asAddaxException(
                                    CONFIG_ERROR,
                                    String.format(
                                            "列配置信息有错误. 表中未找到列名 '%s' .",
                                            colunmnName));
                }
                columnTypes.add(col.getType());
            }
            if (writeTimeCol != -1) {
                insertStmt.using(timestamp(QueryBuilder.bindMarker()));
            }
            String cl = taskConfig.getString(CassandraKey.CONSISTENCY_LEVEL);
            if (cl != null && !cl.isEmpty()) {
                insertStmt.setConsistencyLevel(ConsistencyLevel.valueOf(cl));
            }
            else {
                insertStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            }

            statement = session.prepare(insertStmt);

            if (batchSize > 1) {
                if (asyncWrite) {
                    unConfirmedWrite = new ArrayList<>();
                }
                else {
                    bufferedWrite = new ArrayList<>();
                }
            }
        }

        @Override
        public void destroy()
        {

        }
    }
}
