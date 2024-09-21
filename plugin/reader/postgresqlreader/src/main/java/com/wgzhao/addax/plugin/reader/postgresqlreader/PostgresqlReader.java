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

package com.wgzhao.addax.plugin.reader.postgresqlreader;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.common.base.Key.FETCH_SIZE;
import static com.wgzhao.addax.common.exception.ErrorCode.ILLEGAL_VALUE;

public class PostgresqlReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;

    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig;
        private CommonRdbmsReader.Job commonRdbmsReaderMaster;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            int fetchSize = this.originalConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("您配置的fetchSize有误，fetchSize : [%d] 设置值不能小于 1.", fetchSize));
            }
            this.originalConfig.set(FETCH_SIZE, fetchSize);

            this.commonRdbmsReaderMaster = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.originalConfig = this.commonRdbmsReaderMaster.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return this.commonRdbmsReaderMaster.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderMaster.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderMaster.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderSlave;

        @Override
        public void init()
        {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderSlave = new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId())
            {
                @Override
                protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                        throws SQLException, UnsupportedEncodingException
                {
                    if (metaData.getColumnType(i) == Types.DOUBLE && metaData.isCurrency(i)) {
                        // money type has currency symbol( etc $) and thousands separator(,)
                        return new DoubleColumn(Double.valueOf(rs.getString(i).substring(1).replace(",","")));
                    }
                    return super.createColumn(rs, metaData, i);
                }

            };
            this.commonRdbmsReaderSlave.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig.getInt(FETCH_SIZE);

            this.commonRdbmsReaderSlave.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderSlave.post(this.readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderSlave.destroy(this.readerSliceConfig);
        }
    }
}
