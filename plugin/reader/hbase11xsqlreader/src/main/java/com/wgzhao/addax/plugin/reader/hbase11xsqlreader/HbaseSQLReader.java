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

package com.wgzhao.addax.plugin.reader.hbase11xsqlreader;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;

public class HbaseSQLReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig;
        private HbaseSQLHelper hbaseSQLHelper;

        @Override
        public void init()
        {
            hbaseSQLHelper = new HbaseSQLHelper(this.getPluginJobConf());
        }

        @Override
        public void prepare()
        {
            originalConfig = hbaseSQLHelper.parseConfig();
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> splitResultConfigs = new ArrayList<>();
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(originalConfig.clone());
            }
            return splitResultConfigs;
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
        private Configuration writerConfig;

        @Override
        public void init()
        {
            this.writerConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.info("Begin reading.....");
            PreparedStatement preparedStatement;
            ResultSet resultSet;
            Record record;
            String querySql = this.writerConfig.getString(Key.QUERY_SQL);
            String jdbcUrl = this.writerConfig.getString(Key.JDBC_URL);
            LOG.info("Query with [{}]", querySql);
            try {
                Connection connection = DriverManager.getConnection(jdbcUrl);
                preparedStatement = connection.prepareStatement(querySql);
                resultSet = preparedStatement.executeQuery();
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
                    record = transportOneRecord(recordSender, resultSet, metaData, metaData.getColumnCount(), getTaskPluginCollector());
                    recordSender.sendToWriter(record);
                }
                recordSender.flush();
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        e.getMessage()
                );
            }
            LOG.info("End reading.....");
        }

        private Record transportOneRecord(RecordSender recordSender, ResultSet resultSet, ResultSetMetaData rmd, int columnNum, TaskPluginCollector taskPluginCollector)
        {
            Record record = recordSender.createRecord();
            Column column;
            try {
                for (int i = 1; i <= columnNum; i++) {
                    Object value = resultSet.getObject(i);
                    switch (rmd.getColumnType(i)) {
                        case Types.CHAR:
                        case Types.VARCHAR:
                            column = new StringColumn((String) value);
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                            column = new BytesColumn((byte[]) value);
                            break;
                        case Types.BOOLEAN:
                            column = new BoolColumn((Boolean) value);
                            break;
                        case Types.INTEGER:
                            column = new LongColumn((Integer) value);
                            break;
                        case Types.TINYINT:
                            column = new LongColumn(((Byte) value).longValue());
                            break;
                        case Types.SMALLINT:
                            column = new LongColumn(((Short) value).longValue());
                            break;
                        case Types.BIGINT:
                            column = new LongColumn((Long) value);
                            break;
                        case Types.FLOAT:
                            column = new DoubleColumn(((Float) value).doubleValue());
                            break;
                        case Types.DECIMAL:
                            column = new DoubleColumn(((BigDecimal) value));
                            break;
                        case Types.DOUBLE:
                            column = new DoubleColumn((Double) value);
                            break;
                        case Types.DATE:
                            column = new DateColumn((Date) value);
                            break;
                        case Types.TIME:
                            column = new DateColumn((Time) value);
                            break;
                        case Types.TIMESTAMP:
                            column = new DateColumn((Timestamp) value);
                            break;
                        default:
                            throw AddaxException.asAddaxException(
                                    NOT_SUPPORT_TYPE, "The data type " + rmd.getColumnType(i) + " is unsupported yet");
                    }
                    record.addColumn(column);
                }
                return record;
            }
            catch (SQLException e) {
                taskPluginCollector.collectDirtyRecord(record, e);
                return null;
            }
        }
    }
}
