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

package com.wgzhao.addax.plugin.writer.sqlitewriter;

import com.wgzhao.addax.common.base.Key;
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
import static com.wgzhao.addax.common.base.Key.PASSWORD;
import static com.wgzhao.addax.common.base.Key.USERNAME;

public class SqliteWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.SQLite;

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
            this.originalConfig = super.getPluginJobConf();
            // SQLite does not minder username and password
            this.originalConfig.set(USERNAME, "addax");
            this.originalConfig.set(PASSWORD, "");
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post()
        {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
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
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE) {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                        int columnSqlType, Column column)
                        throws SQLException
                {
                    if (column == null || column.getRawData() == null) {
                        preparedStatement.setObject(columnIndex, null);
                        return preparedStatement;
                    }
                    if (columnSqlType == Types.DATE) {
                        // SQLite does not have a storage class set aside for storing dates and/or times.
                        // Instead, the built-in Date And Time Functions of SQLite are capable of storing dates and times as
                        // TEXT, REAL, or INTEGER values: https://www.sqlite.org/datatype3.html
                        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
                        preparedStatement.setString(columnIndex, sdf.format(column.asDate()));
                        return preparedStatement;
                    }
                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }
    }
}
