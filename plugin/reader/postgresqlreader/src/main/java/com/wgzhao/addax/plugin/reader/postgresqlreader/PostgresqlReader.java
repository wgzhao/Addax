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

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.DataWrapper;
import org.postgis.PGgeometry;
import org.postgresql.util.PGobject;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.List;
import java.util.Objects;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.core.base.Key.FETCH_SIZE;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

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
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "the fetchSize can not be less than 1.");
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
                        return new DoubleColumn(Double.valueOf(rs.getString(i).substring(1).replace(",", "")));
                    }
                    else if (metaData.getColumnType(i) == Types.OTHER) {
                        Object object = rs.getObject(i);
                        // only handle PGobject, others will be handled in super class
                        if (object instanceof PGobject && !(object instanceof PGgeometry)) {
                            DataWrapper dataWrapper = new DataWrapper();
                            dataWrapper.setRawData(JSON.toJSONString(object));
                            dataWrapper.setColumnTypeName(metaData.getColumnTypeName(i));
                            return new BytesColumn(JSON.toJSONBytes(dataWrapper));
                        }
                    }
                    else if (metaData.getColumnType(i) == Types.ARRAY) {
                        Array dataArray = rs.getArray(i);
                        if (Objects.isNull(dataArray)) {
                            return new BytesColumn(null);
                        }
                        DataWrapper pgWrapperForArray = new DataWrapper();
                        pgWrapperForArray.setColumnTypeName(dataArray.getBaseTypeName());
                        pgWrapperForArray.setRawData(dataArray.toString());
                        return new BytesColumn(JSON.toJSONBytes(pgWrapperForArray));
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
