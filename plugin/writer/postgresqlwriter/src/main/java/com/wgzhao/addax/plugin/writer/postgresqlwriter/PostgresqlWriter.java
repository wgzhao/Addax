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

package com.wgzhao.addax.plugin.writer.postgresqlwriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;

public class PostgresqlWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                if (!"insert".equalsIgnoreCase(writeMode)
                        && !writeMode.startsWith("update")) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            String.format("写入模式(writeMode)配置错误. PostgreSQL 仅支持insert, update两种模式." +
                                            " %s 不支持",
                                    writeMode));
                }
            }

            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init()
        {
            this.writerSliceConfig = getPluginJobConf();
            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE)
            {
                @Override
                public String calcValueHolder(String columnType)
                {
                    if ("serial".equalsIgnoreCase(columnType)) {
                        return "?::INT";
                    }
                    else if ("bit".equalsIgnoreCase(columnType)) {
                        return "?::BIT VARYING";
                    }
                    else if ("bigserial".equalsIgnoreCase(columnType)) {
                        return "?::BIGINT";
                    }
                    else if ("xml".equalsIgnoreCase(columnType)) {
                        return "?::XML";
                    }
                    else if ("money".equalsIgnoreCase(columnType)) {
                        return "?::NUMERIC::MONEY";
                    }
                    else if ("bool".equalsIgnoreCase(columnType)) {
                        return "?::BOOLEAN";
                    }
                    return super.calcValueHolder(columnType);
                }

                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqlType, Column column)
                        throws SQLException
                {
                    if (column == null || column.getRawData() == null) {
                        preparedStatement.setObject(columnIndex, null);
                        return preparedStatement;
                    }

                    if (columnSqlType == Types.BIT) {
                        String v;
                        if (column.getType() == Column.Type.BOOL) {
                           v =  column.asBoolean() ? "1" : "0";
                        } else {
                            v = bytes2Binary(column.asBytes());
                        }
                        preparedStatement.setString(columnIndex, v);
                        return preparedStatement;
                    }

                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };

            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        private String bytes2Binary(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
            }
            return sb.toString();
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }
    }
}
