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

package com.wgzhao.addax.plugin.writer.oraclewriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

public class OracleWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck()
        {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            String writeMode = originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                if (!"insert".equalsIgnoreCase(writeMode) && !writeMode.startsWith("update")) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            String.format("The item writeMode your configured [%s] is unsupported, it only supports insert and update mode.", writeMode));
                }
            }

            commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            commonRdbmsWriterJob.init(originalConfig);
        }

        @Override
        public void prepare()
        {
            commonRdbmsWriterJob.prepare(originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return commonRdbmsWriterJob.split(originalConfig, mandatoryNumber);
        }

        @Override
        public void post()
        {
            commonRdbmsWriterJob.post(originalConfig);
        }

        @Override
        public void destroy()
        {
            commonRdbmsWriterJob.destroy(originalConfig);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init()
        {
            this.writerSliceConfig = getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE) {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqlType, Column column)
                        throws SQLException
                {
                    if (writerSliceConfig.getString(Key.WRITE_MODE, "").startsWith("update")) {
                        if (columnSqlType == Types.CLOB ) {
                            Clob clob = preparedStatement.getConnection().createClob();
                            clob.setString(1, column.asString());
                            preparedStatement.setClob(columnIndex, clob);
                            return preparedStatement;
                        }
                        if (columnSqlType == Types.BLOB) {
                            Blob blob = preparedStatement.getConnection().createBlob();
                            blob.setBytes(1, column.asBytes());
                            preparedStatement.setBlob(columnIndex, blob);
                            return preparedStatement;
                        }
                        return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                    }

                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };
            commonRdbmsWriterTask.init(writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            commonRdbmsWriterTask.prepare(writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            commonRdbmsWriterTask.startWrite(recordReceiver, writerSliceConfig, getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            commonRdbmsWriterTask.post(writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            commonRdbmsWriterTask.destroy(writerSliceConfig);
        }
    }
}
