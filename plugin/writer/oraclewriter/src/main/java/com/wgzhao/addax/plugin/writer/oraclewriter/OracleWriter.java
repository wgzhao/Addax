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

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

/** Oracle Writer. */
public class OracleWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    /** Job. */
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

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        /**
         * The largest value the merge template can bind directly. Past a TTC field the server drops
         * the connection with ORA-03146 / ORA-03138 / ORA-03106 instead of answering with an error,
         * which is what issues #1030 and #1092 report. Measured against 21c XE with ojdbc 19.18:
         * a merge binds at most 8191 characters of text or 32767 bytes of binary, and the limit
         * counts characters, not encoded bytes (8000 CJK characters, 24000 bytes, still bind).
         */
        private static final int MAX_INLINE_CLOB_CHARS = 8191;

        private static final int MAX_INLINE_BLOB_BYTES = 32767;

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        /** temporary LOBs built while binding the current batch, released once it has executed */
        private final List<Blob> pendingBlobs = new ArrayList<>();
        private final List<Clob> pendingClobs = new ArrayList<>();

        @Override
        public void init()
        {
            this.writerSliceConfig = getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE)
            {
                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqlType, Column column)
                        throws SQLException
                {
                    if (columnSqlType == Types.NVARCHAR || columnSqlType == Types.NCHAR) {
                        preparedStatement.setNString(columnIndex, column.asString());
                        return preparedStatement;
                    }

                    // An INSERT resolves the type of a placeholder from the target column, so the
                    // server turns a value of any size into the LOB column value. The MERGE template
                    // resolves it from the surrounding expression instead, and that is the only
                    // statement shape that has to stage a large value in a temporary LOB first.
                    if (writeMode == null || !writeMode.toLowerCase().startsWith("update")) {
                        return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                    }

                    if (columnSqlType == Types.CLOB || columnSqlType == Types.NCLOB) {
                        String value = column.asString();
                        if (value == null || value.length() <= MAX_INLINE_CLOB_CHARS) {
                            return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                        }
                        Clob clob = preparedStatement.getConnection().createClob();
                        clob.setString(1, value);
                        preparedStatement.setClob(columnIndex, clob);
                        pendingClobs.add(clob);
                        return preparedStatement;
                    }
                    if (columnSqlType == Types.BLOB) {
                        byte[] value = column.asBytes();
                        if (value == null || value.length <= MAX_INLINE_BLOB_BYTES) {
                            return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                        }
                        Blob blob = preparedStatement.getConnection().createBlob();
                        blob.setBytes(1, value);
                        preparedStatement.setBlob(columnIndex, blob);
                        pendingBlobs.add(blob);
                        return preparedStatement;
                    }
                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }

                @Override
                protected void doBatchInsert(Connection connection, List<Record> buffer, boolean supportCommit)
                        throws SQLException
                {
                    try {
                        super.doBatchInsert(connection, buffer, supportCommit);
                    }
                    finally {
                        // the locators have reached the server by now; keeping the temporary
                        // LOBs any longer only holds temp tablespace for the life of the connection
                        releaseLobs();
                    }
                }
            };
            commonRdbmsWriterTask.init(writerSliceConfig);
        }

        private void releaseLobs()
        {
            for (Blob blob : pendingBlobs) {
                try {
                    blob.free();
                }
                catch (SQLException e) {
                    LOG.warn("Failed to free a temporary BLOB: {}", e.getMessage());
                }
            }
            pendingBlobs.clear();
            for (Clob clob : pendingClobs) {
                try {
                    clob.free();
                }
                catch (SQLException e) {
                    LOG.warn("Failed to free a temporary CLOB: {}", e.getMessage());
                }
            }
            pendingClobs.clear();
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
