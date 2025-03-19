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

package com.wgzhao.addax.plugin.reader.clickhousereader;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_FETCH_SIZE;

public class ClickHouseReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.ClickHouse;

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            this.originalConfig.set(Key.FETCH_SIZE, DEFAULT_FETCH_SIZE);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.originalConfig = this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck()
        {
            this.commonRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);
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
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId())
            {
                @Override
                protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                        throws SQLException, UnsupportedEncodingException
                {
                    int dataType = metaData.getColumnType(i);
                    // Please to use java.time.LocalDateTime or java.time.OffsetDateTime instead of java.sql.Timestamp,
                    // and java.time.LocalDate instead of java.sql.Date.
                    // references https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc
                    if (dataType == Types.TIMESTAMP) {
//                        return new TimestampColumn(Timestamp.valueOf((LocalDateTime) rs.getObject(i)));
                        return new TimestampColumn(Timestamp.valueOf(rs.getObject(i, java.time.LocalDateTime.class)));
                    }
                    else if (dataType == Types.OTHER) {
                        // database-specific type, convert it to string as default
                        String dType = metaData.getColumnTypeName(i);
                        if (dType.startsWith("DateTime")) {
                            return new TimestampColumn(Timestamp.valueOf((LocalDateTime) rs.getObject(i)));
                        }
                        else {
                            return new StringColumn(rs.getObject(i).toString());
                        }
                    }
                    else {
                        return super.createColumn(rs, metaData, i);
                    }
                }
            };
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig.getInt(Key.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
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
