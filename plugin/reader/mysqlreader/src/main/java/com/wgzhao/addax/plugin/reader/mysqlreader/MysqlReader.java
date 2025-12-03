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

package com.wgzhao.addax.plugin.reader.mysqlreader;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.ErrorCode;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class MysqlReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            Integer userConfiguredFetchSize = this.originalConfig.getInt(Key.FETCH_SIZE);
            if (userConfiguredFetchSize != null) {
                LOG.warn("The plugin not support fetchSize config, fetchSize will be forced to -1(ignore).");
            }

            this.originalConfig.set(Key.FETCH_SIZE, Integer.MIN_VALUE);

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
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId())
            {
                @Override
                protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                        throws SQLException, UnsupportedEncodingException
                {
                    if (metaData.getColumnType(i) == Types.DATE && "YEAR".equals(metaData.getColumnTypeName(i))) {
                        return new LongColumn(rs.getLong(i));
                    }
                    if (metaData.getColumnType(i) == Types.BINARY && "GEOMETRY".equals(metaData.getColumnTypeName(i))) {
                        WKBReader wkbReader = new WKBReader();
                        try {
                            byte[] wkbWithSRID = rs.getBytes(i);
                            // If the column is SQL NULL or empty, return a NULL StringColumn to avoid NPE
                            if (wkbWithSRID == null || wkbWithSRID.length == 0) {
                                return new StringColumn((String) null);
                            }
                            // Remove the SRID prefix (4 bytes) if present
                            if (wkbWithSRID.length > 4) {
                                byte[] wkbWithoutSRID = new byte[wkbWithSRID.length - 4];
                                System.arraycopy(wkbWithSRID, 4, wkbWithoutSRID, 0, wkbWithoutSRID.length);
                                wkbWithSRID = wkbWithoutSRID;
                            } else {
                                // Only 4 bytes or fewer, no actual WKB payload, treat as NULL
                                return new StringColumn((String) null);
                            }
                            // Double-check to be safe before parsing
                            Geometry geometry = wkbReader.read(wkbWithSRID);
                            return new StringColumn(geometry.toText());
                        }
                        catch (ParseException e) {
                            throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR,
                                    String.format("Failed to parse WKB data in column %d: %s", i, e.getMessage()), e);
                        }
                    }
                    return super.createColumn(rs, metaData, i);
                }
            };
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig.getInt(Key.FETCH_SIZE);

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
