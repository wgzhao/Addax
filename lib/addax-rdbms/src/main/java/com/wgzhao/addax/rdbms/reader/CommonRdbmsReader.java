/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.rdbms.reader;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.statistics.PerfRecord;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.reader.util.GetPrimaryKeyUtil;
import com.wgzhao.addax.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.wgzhao.addax.rdbms.reader.util.PreCheckTask;
import com.wgzhao.addax.rdbms.reader.util.ReaderSplitUtil;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;

/**
 * Common RDBMS Reader implementation providing database reading capabilities.
 * Supports both table mode and query SQL mode for flexible data extraction.
 */
public class CommonRdbmsReader
{
    /**
     * Job-level operations for RDBMS reader including initialization, validation, and task splitting.
     */
    public static class Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private final DataBaseType dataBaseType;

        /**
         * Constructs a new Job instance for the specified database type.
         *
         * @param dataBaseType The database type this job will operate on
         */
        public Job(DataBaseType dataBaseType)
        {
            this.dataBaseType = dataBaseType;
        }

        /**
         * Initializes the job configuration and performs preprocessing.
         * Attempts to auto-detect primary key for splitting if configured.
         *
         * @param originalConfig The original job configuration
         * @return The processed configuration
         */
        public Configuration init(Configuration originalConfig)
        {
            OriginalConfPretreatmentUtil.doPretreatment(dataBaseType, originalConfig);
            if (Objects.equals(originalConfig.getString(Key.SPLIT_PK, ""), "") && originalConfig.getBool(Key.AUTO_PK, false)) {
                LOG.info("The split key is not configured, try to guess the split key.");
                String splitPK = GetPrimaryKeyUtil.getPrimaryKey(dataBaseType, originalConfig);
                if (splitPK != null) {
                    LOG.info("Take the field {} as split key", splitPK);
                    originalConfig.set(Key.SPLIT_PK, splitPK);
                }
                else {
                    LOG.warn("There is no primary key or unique key in the table, and the split key cannot be guessed.");
                }
            }

            LOG.debug("After the job is initialized, the job configuration is now as follows::[\n{}\n]", originalConfig.toJSON());
            return originalConfig;
        }

        /**
         * Performs pre-checks on the configuration to validate table accessibility and split key validity.
         *
         * @param originalConfig The configuration to validate
         * @param dataBaseType The database type
         * @throws AddaxException if pre-check validation fails
         */
        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType)
        {
            // Check each table can be read and split key is valid
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            Configuration connConf = queryConf.getConfiguration(Key.CONNECTION);
            String username = queryConf.getString(Key.USERNAME);
            String password = queryConf.getString(Key.PASSWORD);
            new PreCheckTask(username, password, connConf, dataBaseType, splitPK).call();
        }

        /**
         * Splits the job configuration into multiple task configurations.
         *
         * @param originalConfig The original job configuration
         * @param adviceNumber The suggested number of splits
         * @return List of task configurations
         */
        public List<Configuration> split(Configuration originalConfig, int adviceNumber)
        {
            return ReaderSplitUtil.doSplit(dataBaseType, originalConfig, adviceNumber);
        }

        /**
         * Performs post-processing operations after all tasks are completed.
         *
         * @param originalConfig The original job configuration
         */
        public void post(Configuration originalConfig)
        {
            // No post-processing required
        }

        /**
         * Cleans up resources after job completion.
         *
         * @param originalConfig The original job configuration
         */
        public void destroy(Configuration originalConfig)
        {
            // No cleanup required
        }
    }

    /**
     * Task-level operations for reading data from RDBMS sources.
     * Handles the actual data reading and record transformation.
     */
    public static class Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        /** Empty byte array constant used for string encoding operations when source bytes are null */
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private final DataBaseType dataBaseType;
        private final int taskGroupId;
        private final int taskId;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        private String basicMsg;

        /**
         * Creates a new Task instance for the specified database type.
         *
         * @param dataBaseType The database type this task will operate on
         */
        public Task(DataBaseType dataBaseType)
        {
            this(dataBaseType, -1, -1);
        }

        /**
         * Creates a new Task instance with group and task identifiers.
         *
         * @param dataBaseType The database type this task will operate on
         * @param taskGroupId The task group identifier
         * @param taskId The task identifier
         */
        public Task(DataBaseType dataBaseType, int taskGroupId, int taskId)
        {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGroupId;
            this.taskId = taskId;
        }

        /**
         * Initializes the task with configuration parameters.
         *
         * @param readerSliceConfig The slice configuration for this task
         */
        public void init(Configuration readerSliceConfig)
        {
            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            basicMsg = "jdbcUrl: " + this.jdbcUrl;
        }

        /**
         * read data
         *
         * @param readerSliceConfig The read configuration
         * @param recordSender The record sender
         * @param taskPluginCollector The task plugin collector
         * @param fetchSize The fetch size
         */
        public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
                TaskPluginCollector taskPluginCollector, int fetchSize)
        {
            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);

            LOG.info("Begin reading records by executing SQL query: [{}].", querySql);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

            // session config related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig, this.dataBaseType, basicMsg);

            int columnNumber;
            ResultSet rs;
            try {
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    transportOneRecord(recordSender, rs, metaData, columnNumber, taskPluginCollector);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                LOG.info("Finished reading records by executing SQL query: [{}].", querySql);
            }
            catch (Exception e) {
                throw RdbmsException.asQueryException(e, querySql);
            }
            finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        /**
         * Performs task cleanup operations.
         * This method is called after data reading is complete.
         *
         * @param originalConfig The reader configuration
         */
        public void post(Configuration originalConfig)
        {
            // do nothing
        }

        /**
         * Performs task cleanup and resource deallocation.
         * This method is called when the task is being shut down.
         *
         * @param originalConfig The reader configuration
         */
        public void destroy(Configuration originalConfig)
        {
            // do nothing
        }

        /**
         * Transports one record from ResultSet to RecordSender.
         * This method builds a record from the current row in the ResultSet and sends it downstream.
         *
         * @param recordSender Interface to send records downstream
         * @param rs The ResultSet positioned at the current row
         * @param metaData Metadata about the ResultSet columns
         * @param columnNumber Number of columns in the ResultSet
         * @param taskPluginCollector Collector for handling dirty records
         */
        protected void transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData,
                int columnNumber, TaskPluginCollector taskPluginCollector)
        {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, taskPluginCollector);
            recordSender.sendToWriter(record);
        }

        /**
         * create column
         *
         * @param rs The result set
         * @param metaData The result set meta data
         * @param i The column index
         * @return The column
         * @throws SQLException If an SQL exception occurs
         * @throws UnsupportedEncodingException If the encoding is not supported
         */
        protected Column createColumn(ResultSet rs, ResultSetMetaData metaData, int i)
                throws SQLException, UnsupportedEncodingException
        {
            switch (metaData.getColumnType(i)) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    String rawData;
                    if (StringUtils.isBlank(mandatoryEncoding)) {
                        rawData = rs.getString(i);
                    }
                    else {
                        rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : rs.getBytes(i)), mandatoryEncoding);
                    }
                    return new StringColumn(rawData);

                case Types.CLOB:
                case Types.NCLOB:
                    return new StringColumn(rs.getString(i));

                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    return new LongColumn(rs.getString(i));

                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    return new DoubleColumn(rs.getString(i));

                case Types.TIME:
                    return new DateColumn(rs.getTime(i));

                case Types.DATE:
                    return new DateColumn(rs.getDate(i));

                case Types.TIMESTAMP:
                    return new TimestampColumn(rs.getTimestamp(i, Calendar.getInstance()));

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    return new BytesColumn(rs.getBytes(i));

                case Types.BOOLEAN:
                    return new BoolColumn(rs.getBoolean(i));

                case Types.BIT:
                    // bit(1) -> Types.BIT  use BooleanColumn
                    // bit(>1) -> Types.VARBINARY use BytesColumn
                    if (metaData.getPrecision(i) == 1) {
                        return new BoolColumn(rs.getBoolean(i));
                    }
                    else {
                        return new BytesColumn(rs.getBytes(i));
                    }

                case Types.ARRAY:
                    return new StringColumn(Objects.isNull(rs.getObject(i)) ? null : rs.getArray(i).toString());

                case Types.SQLXML:
                    return new StringColumn(rs.getSQLXML(i).getString());

                default:
                    // use object as default data type for all unknown datatype
                    LOG.debug("Unknown data type: {} at field name: {}, using getObject().", metaData.getColumnType(i), metaData.getColumnName(i));
                    String stringData = null;
                    if (rs.getObject(i) != null) {
                        stringData = rs.getObject(i).toString();
                    }
                    return new StringColumn(stringData);
            }
        }

        /**
         * build record
         *
         * @param recordSender The record sender
         * @param rs The result set
         * @param metaData The result set meta data
         * @param columnNumber The column number
         * @param taskPluginCollector The task plugin collector
         * @return The record
         */
        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber,
                TaskPluginCollector taskPluginCollector)
        {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    record.addColumn(createColumn(rs, metaData, i));
                }
            }
            catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("Exception occurred while reading {} : {}", record, e.getMessage());
                }
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof AddaxException) {
                    throw (AddaxException) e;
                }
            }
            return record;
        }
    }
}
