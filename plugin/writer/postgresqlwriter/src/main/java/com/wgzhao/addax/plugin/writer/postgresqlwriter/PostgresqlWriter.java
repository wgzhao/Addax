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

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.DataWrapper;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

public class PostgresqlWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;
    private static final Logger log = LoggerFactory.getLogger(PostgresqlWriter.class);

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
                            "The writeMode should be insert or update or updateAndInsert, but not : " + writeMode);
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
        private List<Integer> hasZColumns;

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
                            v = column.asBoolean() ? "1" : "0";
                        }
                        else {
                            v = bytes2Binary(column.asBytes());
                        }
                        preparedStatement.setString(columnIndex, v);
                        return preparedStatement;
                    }
                    else if (columnSqlType == Types.OTHER) {
                        Object rawData = column.getRawData();
                        if (Objects.isNull(rawData)) {
                            preparedStatement.setNull(columnIndex, Types.OTHER);
                            return preparedStatement;
                        }
                        if (rawData instanceof byte[]) {
                            // only handle PGobject, others will be handled in super class
                            DataWrapper dataWrapper = JSON.parseObject(column.asBytes(), DataWrapper.class);
                            if (Objects.nonNull(dataWrapper)) {
                                Object pgRawData = dataWrapper.getRawData();
                                String columnTypeName = dataWrapper.getColumnTypeName();
                                if (PostGisColumnTypeName.isPGObject(columnTypeName)) {
                                    PGobject pgObject = JSON.parseObject((String) pgRawData, PGobject.class);
                                    preparedStatement.setObject(columnIndex, pgObject);
                                    return preparedStatement;
                                }
                            }
                        }
                        else if (rawData instanceof String) {
                            if (column.getType() == Column.Type.STRING) {
                                if (Objects.nonNull(hasZColumns) && hasZColumns.contains(columnIndex)) {
                                    String original2D = (String) rawData;
                                    if(original2D.contains("EMPTY")) {
                                        String zmEmptyGeometry = original2D.replace("EMPTY", "ZM EMPTY");
                                        preparedStatement.setObject(columnIndex, zmEmptyGeometry, Types.OTHER);
                                        return preparedStatement;
                                    }
                                }
                            }
                        }
                    }
                    else if (columnSqlType == Types.ARRAY) {
                        Object rawData = column.getRawData();
                        if (Objects.isNull(rawData)) {
                            preparedStatement.setNull(columnIndex, Types.ARRAY);
                            return preparedStatement;
                        }
                        if (rawData instanceof byte[]) {
                            DataWrapper dataWrapper = JSON.parseObject(column.asBytes(), DataWrapper.class);
                            if (Objects.nonNull(dataWrapper)) {
                                Object pgRawData = dataWrapper.getRawData();
                                String columnTypeName = dataWrapper.getColumnTypeName();
                                String arrayStr = (String) pgRawData;
                                if (arrayStr.startsWith("{") && arrayStr.endsWith("}")) {
                                    arrayStr = arrayStr.substring(1, arrayStr.length() - 1);
                                }
                                String[] elements = arrayStr.split(",");
                                Object[] pgArray = new Object[elements.length];
                                for (int i = 0; i < elements.length; i++) {
                                    pgArray[i] = elements[i].trim();
                                }
                                preparedStatement.setArray(columnIndex,
                                        preparedStatement.getConnection().createArrayOf(
                                                columnTypeName, pgArray));
                            }
                            else {
                                log.error("type array deserialized PostgisWrapper is null, please check your database data.");
                                preparedStatement.setNull(columnIndex, Types.ARRAY);
                            }
                            return preparedStatement;
                        }
                    }
                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };

            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
            this.hasZColumns = writerSliceConfig.getList(Key.HAS_Z_COLUMN, Integer.class);
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
