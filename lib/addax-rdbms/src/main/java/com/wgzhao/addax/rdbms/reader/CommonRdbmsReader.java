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
import java.math.BigInteger;
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
        private static final int SQL_LOG_MAX_LENGTH = 256;
        // Calendar is not thread-safe while reader tasks run in parallel, so each
        // thread converts timestamps with its own instance of the default timezone
        private static final ThreadLocal<Calendar> CALENDAR_INSTANCE = ThreadLocal.withInitial(Calendar::getInstance);
        // compared against every BIGINT UNSIGNED cell, so it must not be re-allocated per row
        private static final BigInteger BIGINT_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

        private final DataBaseType dataBaseType;
        private final int taskGroupId;
        private final int taskId;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        private String basicMsg;

        // per-column metadata of the ResultSet currently being read; all values are
        // row-invariant and are materialized once per ResultSet instead of per row
        private ResultSetMetaData cachedMetaData;
        private int[] cachedColTypes;
        private boolean[] cachedSigned;
        private int[] cachedScales;
        private int[] cachedPrecisions;
        private String[] cachedTypeNames;
        private String[] cachedColumnNames;

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
            String logSql = querySql.length() > SQL_LOG_MAX_LENGTH ?
                    StringUtils.abbreviate(querySql, SQL_LOG_MAX_LENGTH) : querySql;
            LOG.info("Begin reading records by executing SQL query: [{}].", logSql);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();
            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

            // session config related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig, this.dataBaseType, basicMsg);

            int columnNumber;
            try (ResultSet rs = DBUtil.query(conn, querySql, fetchSize)) {
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
                LOG.info("Finished reading records by executing SQL query: [{}].", logSql);
            }
            catch (Exception e) {
                queryPerfRecord.end();
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
            if (cachedMetaData != metaData) {
                cacheColumnMetaData(metaData);
            }
            int colType = cachedColTypes[i];
            switch (colType) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    if (StringUtils.isBlank(mandatoryEncoding)) {
                        // read the string directly when no explicit encoding is required,
                        // avoiding a redundant getBytes round trip
                        return new StringColumn(rs.getString(i));
                    }
                    byte[] rawBytes = rs.getBytes(i);
                    // a SQL NULL stays NULL instead of being decoded into an empty string
                    return new StringColumn(rawBytes == null ? null : new String(rawBytes, mandatoryEncoding));
                case Types.CLOB:
                case Types.NCLOB:
                    return new StringColumn(rs.getString(i));
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    return new LongColumn(rs.getString(i));
                case Types.BIGINT:
                    if (!cachedSigned[i]) {
                        // BIGINT UNSIGNED may exceed Long.MAX_VALUE (9223372036854775807);
                        // store as StringColumn to preserve full precision
                        String raw = rs.getString(i);
                        if (raw != null) {
                            BigInteger bi = new BigInteger(raw);
                            if (bi.compareTo(BIGINT_LONG_MAX) > 0) {
                                return new StringColumn(raw);
                            }
                            return new LongColumn(bi);
                        }
                        return new LongColumn((String) null);
                    }
                    return new LongColumn(rs.getString(i));
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    return new DoubleColumn(rs.getString(i));
                case Types.TIME:
                    java.sql.Time time = rs.getTime(i);
                    int nanos = 0;
                    // ask the driver for the sub-second component only when the column
                    // actually declares fractional digits, avoiding a second fetch per row
                    if (cachedScales[i] > 0) {
                        try {
                            java.time.LocalTime lt = rs.getObject(i, java.time.LocalTime.class);
                            if (lt != null) {
                                nanos = lt.getNano();
                            }
                        }
                        catch (SQLException | AbstractMethodError | RuntimeException e) {
                            // driver cannot return LocalTime for this column; millis-only value
                            LOG.debug("Failed to read LocalTime of column {}: {}", i, e.getMessage());
                        }
                    }
                    return new DateColumn(time, nanos, cachedScales[i]);
                case Types.DATE:
                    return new DateColumn(rs.getDate(i));
                case Types.TIMESTAMP:
                    return new TimestampColumn(rs.getTimestamp(i, CALENDAR_INSTANCE.get()));
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    return new BytesColumn(rs.getBytes(i));
                case Types.BOOLEAN:
                    return newBoolColumn(rs, i);
                case Types.BIT:
                    // bit(1) -> Types.BIT  use BooleanColumn
                    // bit(>1) -> Types.VARBINARY use BytesColumn
                    if (cachedPrecisions[i] == 1) {
                        return newBoolColumn(rs, i);
                    }
                    return new BytesColumn(rs.getBytes(i));

                case Types.ARRAY:
                    return new StringColumn(Objects.isNull(rs.getObject(i)) ? null : rs.getArray(i).toString());
                case Types.SQLXML: {
                    var xml = rs.getSQLXML(i);
                    // getString on a NULL SQLXML throws, so keep the SQL NULL as a null column
                    return new StringColumn(xml == null ? null : xml.getString());
                }
                default:
                    LOG.debug("Unknown data type: {} (typeName: {}) at field name: {}, using getObject().",
                            colType, cachedTypeNames[i], cachedColumnNames[i]);
                    String stringData = null;
                    if (rs.getObject(i) != null) {
                        stringData = rs.getObject(i).toString();
                    }
                    return new StringColumn(stringData);
            }
        }

        /**
         * getBoolean cannot distinguish a SQL NULL from false, so the wasNull flag decides
         * between a null and a boolean column.
         */
        private Column newBoolColumn(ResultSet rs, int i)
                throws SQLException
        {
            boolean boolValue = rs.getBoolean(i);
            if (rs.wasNull()) {
                return new BoolColumn((Boolean) null);
            }
            return new BoolColumn(boolValue);
        }

        /**
         * Materializes the row-invariant column metadata of a ResultSet once, so that the
         * per-row conversion loop never polls ResultSetMetaData for every cell.
         */
        private void cacheColumnMetaData(ResultSetMetaData metaData)
                throws SQLException
        {
            int columnCount = metaData.getColumnCount();
            int[] colTypes = new int[columnCount + 1];
            boolean[] signed = new boolean[columnCount + 1];
            int[] scales = new int[columnCount + 1];
            int[] precisions = new int[columnCount + 1];
            String[] typeNames = new String[columnCount + 1];
            String[] columnNames = new String[columnCount + 1];
            for (int i = 1; i <= columnCount; i++) {
                colTypes[i] = metaData.getColumnType(i);
                signed[i] = metaData.isSigned(i);
                scales[i] = metaData.getScale(i);
                precisions[i] = metaData.getPrecision(i);
                typeNames[i] = metaData.getColumnTypeName(i);
                columnNames[i] = metaData.getColumnName(i);
            }
            this.cachedMetaData = metaData;
            this.cachedColTypes = colTypes;
            this.cachedSigned = signed;
            this.cachedScales = scales;
            this.cachedPrecisions = precisions;
            this.cachedTypeNames = typeNames;
            this.cachedColumnNames = columnNames;
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
            Column[] columns = new Column[columnNumber];
            try {
                for (int i = 1; i <= columnNumber; i++) {
                    columns[i - 1] = createColumn(rs, metaData, i);
                }
                for (Column col : columns) {
                    record.addColumn(col);
                }
                return record;
            }
            catch (Exception e) {
                LOG.warn("Exception occurred while reading record: {} : {}", record, e.getMessage());
                taskPluginCollector.collectDirtyRecord(record, e);
                return null;
            }
        }
    }
}
