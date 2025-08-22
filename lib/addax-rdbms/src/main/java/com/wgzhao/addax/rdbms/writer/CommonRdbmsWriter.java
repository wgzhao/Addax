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

package com.wgzhao.addax.rdbms.writer;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import com.wgzhao.addax.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.wgzhao.addax.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;

/**
 * Common RDBMS Writer implementation providing database writing capabilities.
 * Supports various write modes including INSERT, REPLACE, and UPDATE operations.
 */
public class CommonRdbmsWriter
{
    /**
     * Job-level operations for RDBMS writer including configuration validation,
     * table preparation, and task coordination.
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
            OriginalConfPretreatmentUtil.dataBaseType = this.dataBaseType;
        }

        /**
         * Initialize job level configuration. Will validate DDL if provided and
         * then perform pretreatment (column expansion, write mode template, etc.).
         *
         * @param originalConfig original job configuration (modified in‑place)
         */
        public void init(Configuration originalConfig)
        {
            ddlValid(originalConfig, dataBaseType);
            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);
            LOG.debug("After job init(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
        }

        /**
         * Pre-check writer configuration: validate pre- / post-SQL syntax and privileges.
         * Currently only MySQL / Oracle / etc. are supported for privilege check.
         *
         * @param originalConfig the original configuration to validate
         * @param dataBaseType the database type for privilege checking
         */
        public void writerPreCheck(Configuration originalConfig, DataBaseType dataBaseType)
        {
            // validate PreSQL & PostSQL syntax
            prePostSqlValid(originalConfig, dataBaseType);
            // validate insert & delete privilege if necessary
            privilegeValid(originalConfig, dataBaseType);
        }

        /**
         * Validate pre and post SQL syntax.
         *
         * @param originalConfig the configuration containing pre/post SQL statements
         * @param dataBaseType the database type for SQL validation
         */
        public void prePostSqlValid(Configuration originalConfig, DataBaseType dataBaseType)
        {
            WriterUtil.preCheckPrePareSQL(originalConfig, dataBaseType);
            WriterUtil.preCheckPostSQL(originalConfig, dataBaseType);
        }

        /**
         * Validate insert / delete privilege.
         *
         * @param originalConfig the configuration containing database connection info
         * @param dataBaseType the database type for privilege checking
         * @throws RdbmsException if user lacks required INSERT or DELETE privileges
         */
        public void privilegeValid(Configuration originalConfig, DataBaseType dataBaseType)
        {
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            Configuration connConf = originalConfig.getConfiguration(Key.CONNECTION);
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            List<String> expandedTables = connConf.getList(Key.TABLE, String.class);
            boolean hasInsertPri = DBUtil.checkInsertPrivilege(dataBaseType, jdbcUrl, username, password, expandedTables);

            if (!hasInsertPri) {
                throw RdbmsException.asInsertPriException(originalConfig.getString(Key.USERNAME), jdbcUrl);
            }

            if (DBUtil.needCheckDeletePrivilege(originalConfig)) {
                boolean hasDeletePri = DBUtil.checkDeletePrivilege(dataBaseType, jdbcUrl, username, password, expandedTables);
                if (!hasDeletePri) {
                    throw RdbmsException.asDeletePriException(originalConfig.getString(Key.USERNAME), jdbcUrl);
                }
            }
        }

        /**
         * Execute user provided DDL before job splitting if configured.
         *
         * @param originalConfig the configuration that may contain DDL statements
         * @param dataBaseType the database type for DDL execution
         */
        public void ddlValid(Configuration originalConfig, DataBaseType dataBaseType)
        {
            if (originalConfig.getString("ddl", null) != null) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);
                Configuration connConf = originalConfig.getConfiguration(Key.CONNECTION);
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                String ddl = originalConfig.getString("ddl");
                Connection connection = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                LOG.info("Executing DDL: {}. context info:{}.", ddl, jdbcUrl);
                WriterUtil.executeSqls(connection, Collections.singletonList(ddl));
                DBUtil.closeDBResources(null, connection);
            }
        }

        /**
         * Prepare job level resources. Normally pre SQL should be delayed to task
         * phase unless there is only one table (single table case executes here).
         *
         * @param originalConfig the original job configuration to prepare
         */
        public void prepare(Configuration originalConfig)
        {
            int tableNumber = originalConfig.getInt(Key.TABLE_NUMBER);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                Configuration connConf = originalConfig.getConfiguration(Key.CONNECTION);

                // jdbcUrl here has already appended suitable suffix parameters
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                originalConfig.set(Key.JDBC_URL, jdbcUrl);

                String table = connConf.getList(Key.TABLE, String.class).get(0);
                originalConfig.set(Key.TABLE, table);

                List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);

                originalConfig.remove(Key.CONNECTION);
                if (!renderedPreSqls.isEmpty()) {
                    originalConfig.remove(Key.PRE_SQL);
                    Connection conn = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                    WriterUtil.executeSqls(conn, renderedPreSqls);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
        }

        /**
         * Split configuration into task slices.
         *
         * @param originalConfig original job configuration to split
         * @param mandatoryNumber the number of parallel channels/tasks to create
         * @return list of task configurations, one per channel
         */
        public List<Configuration> split(Configuration originalConfig, int mandatoryNumber)
        {
            return WriterUtil.doSplit(originalConfig, mandatoryNumber);
        }

        /**
         * Execute post SQL for single table scenario (multi-table handled in task post()).
         *
         * @param originalConfig the original job configuration containing post SQL statements
         */
        public void post(Configuration originalConfig)
        {
            int tableNumber = originalConfig.getInt(Key.TABLE_NUMBER);
            if (tableNumber == 1) {
                String username = originalConfig.getString(Key.USERNAME);
                String password = originalConfig.getString(Key.PASSWORD);

                // jdbcUrl already has appendJDBCSuffix applied by prepare
                String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

                String table = originalConfig.getString(Key.TABLE);

                List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);

                if (!renderedPostSqls.isEmpty()) {
                    originalConfig.remove(Key.POST_SQL);
                    Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);
                    LOG.info("Begin to execute postSqls:[{}]. context info:{}.", StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

        /**
         * Cleanup job level resources.
         *
         * @param originalConfig the original job configuration (unused in current implementation)
         */
        public void destroy(Configuration originalConfig)
        {
            // no-op
        }
    }

    /**
     * Task-level operations for writing data to RDBMS destinations.
     * Handles record buffering, batch insertion, and transaction management.
     */
    public static class Task
    {
        /** Logger instance for this task class */
        protected static final Logger LOG = LoggerFactory.getLogger(Task.class);

        /** Constant for SQL parameter placeholder */
        private static final String VALUE_HOLDER = "?";

        /** Basic message template for logging context information */
        protected static String basicMessage;

        /** Template for INSERT/REPLACE SQL statements */
        protected static String insertOrReplaceTemplate;

        /** Database type for this task */
        protected DataBaseType dataBaseType;

        /** Database username for authentication */
        protected String username;

        /** Database password for authentication */
        protected String password;

        /** JDBC URL for database connection */
        protected String jdbcUrl;

        /** Target table name for data insertion */
        protected String table;

        /** List of column names to insert data into */
        protected List<String> columns;

        /** Pre-SQL statements to execute before data insertion */
        protected List<String> preSqls;

        /** Post-SQL statements to execute after data insertion */
        protected List<String> postSqls;

        /** Number of records to batch before committing */
        protected int batchSize;

        /** Maximum bytes to buffer before committing batch */
        protected int batchByteSize;

        /** Number of columns in the target table */
        protected int columnNumber = 0;

        /** Collector for handling dirty records and statistics */
        protected TaskPluginCollector taskPluginCollector;

        /** Generated SQL statement for record insertion */
        protected String writeRecordSql;

        /** Write mode (INSERT, REPLACE, UPDATE, etc.) */
        protected String writeMode;

        /** Whether to treat empty strings as NULL values */
        protected boolean emptyAsNull;

        /** Metadata information about target table columns */
        protected List<Map<String, Object>> resultSetMetaData;

        /**
         * Constructs a new Task instance for the specified database type.
         *
         * @param dataBaseType The database type this task will operate on
         */
        public Task(DataBaseType dataBaseType)
        {
            this.dataBaseType = dataBaseType;
        }

        /**
         * Initialize task configuration from slice config.
         *
         * @param writerSliceConfig the writer slice configuration containing connection and table info
         */
        public void init(Configuration writerSliceConfig)
        {
            this.username = writerSliceConfig.getString(Key.USERNAME);
            this.password = writerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);

            this.table = writerSliceConfig.getString(Key.TABLE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            this.preSqls = writerSliceConfig.getList(Key.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(Key.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            insertOrReplaceTemplate = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
            this.writeRecordSql = String.format(insertOrReplaceTemplate, this.table);

            basicMessage = "jdbcUrl:" + jdbcUrl + ",table:" + table;
        }

        /**
         * Prepare task (execute pre SQL when multiple tables scenario).
         *
         * @param writerSliceConfig the writer slice configuration containing pre SQL statements
         */
        public void prepare(Configuration writerSliceConfig)
        {
            Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);

            DBUtil.dealWithSessionConfig(connection, writerSliceConfig, this.dataBaseType, basicMessage);

            int tableNumber = writerSliceConfig.getInt(
                    Key.TABLE_NUMBER);
            if (tableNumber != 1) {
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(this.preSqls, ";"), basicMessage);
                WriterUtil.executeSqls(connection, preSqls);
            }

            DBUtil.closeDBResources(null, connection);
        }

        /**
         * Start writing records with provided connection.
         * @param recordReceiver record data source
         * @param taskPluginCollector dirty record collector
         * @param connection JDBC connection
         * @param supportCommit whether commit/rollback is supported
         */
        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection, boolean supportCommit)
        {
            this.taskPluginCollector = taskPluginCollector;
            List<String> mergeColumns = new ArrayList<>();

            if ((this.dataBaseType == DataBaseType.Oracle || this.dataBaseType == DataBaseType.SQLServer)
                    && !"insert".equalsIgnoreCase(this.writeMode)) {
                LOG.info("write {} using {} mode", this.dataBaseType, this.writeMode);
                List<String> columnsOne = new ArrayList<>();
                List<String> columnsTwo = new ArrayList<>();
                String merge = this.writeMode;
                String[] sArray = WriterUtil.getStrings(merge);
                for (String s : this.columns) {
                    if (Arrays.asList(sArray).contains(s)) {
                        columnsOne.add(s);
                    }
                }
                for (String s : this.columns) {
                    if (!Arrays.asList(sArray).contains(s)) {
                        columnsTwo.add(s);
                    }
                }
                int i = 0;
                for (String column : columnsOne) {
                    mergeColumns.add(i++, column);
                }
                for (String column : columnsTwo) {
                    mergeColumns.add(i++, column);
                }
            }
            mergeColumns.addAll(this.columns);

            this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table, StringUtils.join(mergeColumns, ","));

            // combine the insert statement
            calcWriteRecordSql();

            List<Record> writeBuffer = new ArrayList<>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                "The item column number " + record.getColumnNumber() + " in source file not equals the column number " + columnNumber + " in table."
                        );
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(connection, writeBuffer, supportCommit);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer, supportCommit);
                    writeBuffer.clear();
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        EXECUTE_FAIL, e);
            }
            finally {
                writeBuffer.clear();
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        /**
         * Start writing (auto create and manage its own connection, auto commit on success).
         *
         * @param recordReceiver the record receiver to get data from
         * @param writerSliceConfig the writer configuration for this task
         * @param taskPluginCollector the collector for handling dirty records and statistics
         */
        public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig, TaskPluginCollector taskPluginCollector)
        {
            Connection connection = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig, dataBaseType, basicMessage);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connection, true);
        }

        /**
         * Start writing with manual commit control toggle.
         *
         * @param recordReceiver the record receiver to get data from
         * @param writerSliceConfig the writer configuration for this task
         * @param taskPluginCollector the collector for handling dirty records and statistics
         * @param supportCommit whether to support transaction commit/rollback operations
         */
        public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig, TaskPluginCollector taskPluginCollector, boolean supportCommit)
        {
            Connection connection = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig, dataBaseType, basicMessage);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connection, supportCommit);
        }

        /**
         * Execute post SQL (multi-table scenario only).
         *
         * @param writerSliceConfig the writer slice configuration containing post SQL statements
         */
        public void post(Configuration writerSliceConfig)
        {
            int tableNumber = writerSliceConfig.getInt(Key.TABLE_NUMBER);

            boolean hasPostSql = (postSqls != null && !postSqls.isEmpty());
            if (tableNumber == 1 || !hasPostSql) {
                return;
            }

            Connection connection = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);

            LOG.info("Begin to execute postSqls:[{}]. context info:{}.", StringUtils.join(postSqls, ";"), basicMessage);
            WriterUtil.executeSqls(connection, postSqls);
            DBUtil.closeDBResources(null, null, connection);
        }

        /**
         * Clean up task resources.
         *
         * @param writerSliceConfig the writer slice configuration (unused in current implementation)
         */
        public void destroy(Configuration writerSliceConfig)
        {
            // no-op
        }

        /**
         * Batch insert buffered records.
         *
         * @param connection the database connection to use for insertion
         * @param buffer the list of records to insert in batch
         * @param supportCommit whether to support transaction commit/rollback
         * @throws SQLException if database operation fails
         */
        protected void doBatchInsert(Connection connection, List<Record> buffer, boolean supportCommit)
                throws SQLException
        {
            PreparedStatement preparedStatement = null;
            try {
                if (supportCommit) {
                    connection.setAutoCommit(false);
                }
                preparedStatement = connection.prepareStatement(writeRecordSql);
                if ((this.dataBaseType == DataBaseType.Oracle || this.dataBaseType == DataBaseType.SQLServer)
                        && !"insert".equalsIgnoreCase(writeMode)) {
                    String[] sArray = WriterUtil.getStrings(this.writeMode);
                    Set<String> mergeKeySet = new HashSet<>(java.util.Arrays.asList(sArray));
                    for (Record record : buffer) {
                        List<Column> recordOne = new ArrayList<>(this.columns.size() * 2);
                        // key columns first
                        for (int j = 0; j < this.columns.size(); j++) {
                            String col = columns.get(j);
                            if (mergeKeySet.contains(col)) {
                                recordOne.add(record.getColumn(j));
                            }
                        }
                        // non-key columns next
                        for (int j = 0; j < this.columns.size(); j++) {
                            String col = columns.get(j);
                            if (!mergeKeySet.contains(col)) {
                                recordOne.add(record.getColumn(j));
                            }
                        }
                        // all columns again for update placeholders
                        for (int j = 0; j < this.columns.size(); j++) {
                            recordOne.add(record.getColumn(j));
                        }
                        for (int j = 0; j < recordOne.size(); j++) {
                            record.setColumn(j, recordOne.get(j));
                        }
                        preparedStatement = fillPreparedStatement(preparedStatement, record);
                        preparedStatement.addBatch();
                    }
                }
                else {
                    for (Record record : buffer) {
                        preparedStatement = fillPreparedStatement(preparedStatement, record);
                        preparedStatement.addBatch();
                    }
                }
                preparedStatement.executeBatch();
                if (supportCommit) {
                    connection.commit();
                }
            }
            catch (SQLException e) {
                LOG.warn("Rolling back the write, try to write one line at a time. because: {}", e.getMessage());
                if (supportCommit) {
                    connection.rollback();
                }
                doOneInsert(connection, buffer);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        EXECUTE_FAIL, e);
            }
            finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        /**
         * Fallback strategy: insert one by one when batch fails.
         *
         * @param connection the database connection to use for insertion
         * @param buffer the list of records to insert individually
         */
        protected void doOneInsert(Connection connection, List<Record> buffer)
        {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection.prepareStatement(writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(preparedStatement, record);
                        preparedStatement.execute();
                    }
                    catch (SQLException e) {
                        LOG.debug(e.toString());
                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    }
                    finally {
                        // clear parameters before reuse
                        preparedStatement.clearParameters();
                    }
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
            finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        /**
         * Fill PreparedStatement with record values.
         *
         * @param preparedStatement the PreparedStatement to populate
         * @param record the record containing the data to insert
         * @return the populated PreparedStatement
         * @throws SQLException if setting parameter values fails
         */
        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException
        {
            LOG.debug("Record info: {}", record);
            for (int i = 1, len = record.getColumnNumber(); i <= len; i++) {
                int columnSqlType = (int) this.resultSetMetaData.get(i).get("type");
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqlType, record.getColumn(i - 1));
            }
            return preparedStatement;
        }

        /**
         * populate the preparedStatement via the different column type
         * @param preparedStatement the PreparedStatement
         * @param columnIndex the index of the column
         * @param columnSqlType the sql type of the column
         * @param column the column data
         * @return the preparedStatement
         * @throws SQLException if the column type is not supported
         */
        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqlType, Column column)
                throws SQLException
        {
            if (column == null || column.getRawData() == null) {
                preparedStatement.setObject(columnIndex, null);
                return preparedStatement;
            }
            java.util.Date utilDate;
            switch (columnSqlType) {
                case Types.CLOB:
                case Types.CHAR:
                case Types.NCHAR:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.SQLXML:
                case Types.ARRAY:
                    preparedStatement.setString(columnIndex, column.asString());
                    break;

                case Types.BOOLEAN:
                    preparedStatement.setBoolean(columnIndex, column.asBoolean());
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    preparedStatement.setLong(columnIndex, column.asLong());
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    if ((int) this.resultSetMetaData.get(columnIndex).get("scale") == 0) {
                        preparedStatement.setLong(columnIndex, column.asLong());
                    }
                    else {
                        preparedStatement.setBigDecimal(columnIndex, new BigDecimal(column.asString()));
                    }
                    break;

                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    preparedStatement.setDouble(columnIndex, column.asDouble());
                    break;

                case Types.DATE:
                    try {
                        utilDate = column.asDate();
                        preparedStatement.setDate(columnIndex, new java.sql.Date(utilDate.getTime()));
                    }
                    catch (AddaxException e) {
                        throw new SQLException("Failed to convert the column " + column + "to Date type.");
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    }
                    catch (AddaxException e) {
                        throw new SQLException("Failed to convert the column " + column + " to time type.");
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    preparedStatement.setTimestamp(columnIndex, column.asTimestamp());
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex, column.asBytes());
                    break;

                // warn: bit(1) -> Types.BIT using setBoolean
                // warn: bit(>1) -> Types.VARBINARY using setBytes
                case Types.BIT:
                    // bit(1) -> setBoolean; bit(>1) -> treat as VARBINARY and use setBytes
                    if ((int) this.resultSetMetaData.get(columnIndex).get("precision") == 1) {
                        preparedStatement.setBoolean(columnIndex, column.asBoolean());
                    }
                    else {
                        preparedStatement.setBytes(columnIndex, column.asBytes());
                    }
                    break;

                case Types.OTHER:
                    preparedStatement.setObject(columnIndex, column.asString(), Types.OTHER);
                    break;

                default:
                    Map<String, Object> map = this.resultSetMetaData.get(columnIndex);
                    throw AddaxException.asAddaxException(
                            NOT_SUPPORT_TYPE,
                            "Not support the type: field name: " + map.get("name")
                                    + "The SQL type: " + map.get("type")
                                    + "The Java type: " + map.get("typeName"));
            }
            return preparedStatement;
        }

        /**
         * Calculate write record SQL template.
         */
        private void calcWriteRecordSql()
        {
            List<String> valueHolders = new ArrayList<>(columnNumber);
            for (int i = 1; i <= columnNumber; i++) {
                String type = resultSetMetaData.get(i).get("typeName").toString();
                valueHolders.add(calcValueHolder(type));
            }

            insertOrReplaceTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, false);
            writeRecordSql = String.format(insertOrReplaceTemplate, table);
        }

        /**
         * Calculate value holder placeholder for a specific column type.
         *
         * @param columnType the column type name
         * @return the value holder placeholder (typically "?")
         */
        protected String calcValueHolder(String columnType)
        {
            return VALUE_HOLDER;
        }
    }
}
