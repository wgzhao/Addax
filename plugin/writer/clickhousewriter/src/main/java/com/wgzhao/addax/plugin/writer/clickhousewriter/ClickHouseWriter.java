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

package com.wgzhao.addax.plugin.writer.clickhousewriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_DATE_FORMAT;

public class ClickHouseWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.ClickHouse;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
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
            this.writerSliceConfig = super.getPluginJobConf();

            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE)
            {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                        int columnSqlType, Column column)
                        throws SQLException
                {
                    if (column == null || column.getRawData() == null) {
                        preparedStatement.setObject(columnIndex, null);
                        return preparedStatement;
                    }

                    if (columnSqlType == Types.TIMESTAMP) {
                        String columnTypeName = (String) this.resultSetMetaData.get(columnIndex).get("typeName");
                        if (columnTypeName.startsWith("DateTime64(") && columnTypeName.contains(",")) {
                            preparedStatement.setObject(columnIndex, column.asTimestamp()); //setTimestamp is slow and not recommended
                        }
                        else if (columnTypeName.startsWith("DateTime(")) {
                            preparedStatement.setObject(columnIndex, column.asTimestamp());
                        }
                        else if (columnTypeName.equals("DateTime")) {
                            // no precision specified, use default
                            SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
                            preparedStatement.setString(columnIndex, sdf.format(column.asDate()));
                        }
                        else {
                            preparedStatement.setString(columnIndex, column.asString());
                        }
                        return preparedStatement;
                    }
                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };

            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        @Override
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