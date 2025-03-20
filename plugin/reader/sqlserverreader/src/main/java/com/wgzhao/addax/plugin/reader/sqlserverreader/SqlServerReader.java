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

package com.wgzhao.addax.plugin.reader.sqlserverreader;

import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.wgzhao.addax.core.base.Key.FETCH_SIZE;
import static com.wgzhao.addax.core.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

public class SqlServerReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.SQLServer;

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw AddaxException
                        .asAddaxException(ILLEGAL_VALUE, "The fetchSize can not be less than 1");
            }
            this.originalConfig.set(FETCH_SIZE, fetchSize);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.originalConfig = this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId())
            {
                @Override
                protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                        throws SQLException, UnsupportedEncodingException
                {
                    if (metaData.getColumnType(i) == -151) {
                        // compatible with old version SQLServer datetime type
                        return new TimestampColumn(rs.getTimestamp(i));
                    }
                    if (metaData.getColumnType(i) == Types.OTHER && "image".equals(metaData.getColumnTypeName(i))) {
                        return new BytesColumn(rs.getBytes(i));
                    }
                    return super.createColumn(rs, metaData, i);
                }
            };
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig.getInt(FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }
    }
}
