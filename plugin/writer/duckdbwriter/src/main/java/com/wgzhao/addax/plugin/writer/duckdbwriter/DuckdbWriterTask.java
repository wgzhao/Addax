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

package com.wgzhao.addax.plugin.writer.duckdbwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

/**
 * DuckDB specific write path.
 *
 * <p>The appender writes straight into storage instead of going through the SQL layer, which is
 * DuckDB's fastest bulk load path and, unlike a prepared statement, cannot be reached from the
 * generic template driven writer.
 */
class DuckdbWriterTask
        extends CommonRdbmsWriter.Task
{
    private static final Logger LOG = LoggerFactory.getLogger(DuckdbWriterTask.class);

    /**
     * Target types the appender binds directly.
     *
     * <p>Unsigned, binary, nested and timezone aware types are deliberately absent. The driver
     * exposes no overload for them that preserves their meaning in the Addax column model, so
     * they are routed to the prepared statement path rather than risking a value that is
     * plausible but wrong.
     */
    private static final Set<String> APPENDER_TYPES = Set.of(
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "FLOAT", "DOUBLE", "DECIMAL", "VARCHAR",
            "DATE", "TIME", "TIME_NS",
            "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS");

    private static final int APPENDER_UNPROBED = 0;
    private static final int APPENDER_ACTIVE = 1;
    private static final int APPENDER_DISABLED = -1;

    private DuckDBAppender appender;
    private int appenderState = APPENDER_UNPROBED;

    DuckdbWriterTask(DataBaseType dataBaseType)
    {
        super(dataBaseType);
    }

    @Override
    public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector,
            Connection connection, boolean supportCommit)
    {
        try {
            super.startWriteWithConnection(recordReceiver, taskPluginCollector, connection, supportCommit);
        }
        finally {
            // the parent hands the connection back to the pool on the way out, so the appender
            // has to be released while the connection it was opened from is still usable
            closeAppender();
        }
    }

    @Override
    protected void doBatchInsert(Connection connection, List<Record> buffer, boolean supportCommit)
            throws SQLException
    {
        if (appenderState == APPENDER_UNPROBED) {
            appenderState = openAppender(connection) ? APPENDER_ACTIVE : APPENDER_DISABLED;
        }
        if (appenderState == APPENDER_DISABLED) {
            super.doBatchInsert(connection, buffer, supportCommit);
            return;
        }

        try {
            if (supportCommit) {
                connection.setAutoCommit(false);
            }
            for (Record record : buffer) {
                appender.beginRow();
                for (int position = 0; position < columns.size(); position++) {
                    appendColumn(appender, position, record.getColumn(position));
                }
                appender.endRow();
            }
            // flushing per batch bounds the damage of a constraint violation to this batch
            // instead of the appender's internal 204800 row commit interval
            appender.flush();
            if (supportCommit) {
                connection.commit();
            }
        }
        catch (Exception e) {
            // a failed append leaves the appender unusable, so the task stays on the prepared
            // statement path from here on
            LOG.warn("The DuckDB appender rejected a batch ({}), writing with the prepared statement instead", e.getMessage());
            closeAppender();
            appenderState = APPENDER_DISABLED;
            if (supportCommit) {
                connection.rollback();
            }
            // replay the same buffer through the inherited path so a violating row is still
            // singled out as a dirty record instead of failing the whole task
            super.doBatchInsert(connection, buffer, supportCommit);
        }
    }

    /**
     * Retries a failed batch one row at a time.
     *
     * <p>Overridden because DuckDB closes a prepared statement as soon as one of its executions
     * fails, and the shared loop ends with an unconditional {@code clearParameters()} on that
     * statement. That call throws on a closed statement, which aborts the loop, so every row
     * after the offending one would be dropped instead of retried. Preparing per row costs an
     * extra parse, but this path only runs after a batch has already failed.
     */
    @Override
    protected void doOneInsert(Connection connection, List<Record> buffer)
    {
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(true);
            for (Record record : buffer) {
                try {
                    statement = fillPreparedStatement(connection.prepareStatement(writeRecordSql), record);
                    statement.execute();
                }
                catch (SQLException e) {
                    LOG.debug("{}", e.toString());
                    taskPluginCollector.collectDirtyRecord(record, e);
                }
                finally {
                    DBUtil.closeDBResources(statement, null);
                    statement = null;
                }
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
        }
        finally {
            DBUtil.closeDBResources(statement, null);
        }
    }

    /**
     * Creates the appender when the task is shaped for it, and reports why it is not otherwise.
     *
     * @return true when the appender is ready for use
     */
    private boolean openAppender(Connection connection)
    {
        if (!"insert".equalsIgnoreCase(writeMode.trim())) {
            LOG.info("The DuckDB appender only supports insert mode, writeMode [{}] uses the prepared statement", writeMode);
            return false;
        }

        try {
            String mismatch = findColumnMismatch(connection);
            if (mismatch != null) {
                LOG.info("Falling back to the prepared statement, {}", mismatch);
                return false;
            }

            // the appender writes at most one table, named without any quoting or qualification
            String[] parts = unquotedTableParts();
            DuckDBConnection duckConnection = connection.unwrap(DuckDBConnection.class);
            appender = switch (parts.length) {
                case 1 -> duckConnection.createAppender(parts[0]);
                case 2 -> duckConnection.createAppender(parts[0], parts[1]);
                default -> duckConnection.createAppender(parts[0], parts[1], parts[2]);
            };
            LOG.info("Writing into table [{}] with the DuckDB appender", table);
            return true;
        }
        catch (Exception e) {
            LOG.warn("Cannot use the DuckDB appender ({}), writing with the prepared statement instead", e.getMessage());
            closeAppender();
            return false;
        }
    }

    /**
     * Checks the configured columns against the physical table layout.
     *
     * <p>The appender always writes every column of the table in declaration order and cannot be
     * told otherwise, so a reordered or partial column list would silently write values into the
     * wrong columns rather than fail.
     *
     * @return a description of the mismatch, or null when the appender can be used
     */
    private String findColumnMismatch(Connection connection)
    {
        // the metadata list carries a null sentinel at index 0 so that it can be indexed by the
        // 1-based JDBC column position
        List<Map<String, Object>> physical = DBUtil.getColumnMetaData(connection, table, "*");
        if (physical.size() - 1 != columns.size()) {
            return "the table has " + (physical.size() - 1) + " columns " + physicalNames(physical)
                    + " but " + columns.size() + " are configured " + columns;
        }
        for (int position = 0; position < columns.size(); position++) {
            String configured = dataBaseType.unQuote(columns.get(position));
            String actual = String.valueOf(physical.get(position + 1).get("name"));
            if (!configured.equalsIgnoreCase(actual)) {
                return "the configured columns " + columns + " do not match the physical order " + physicalNames(physical);
            }
            String typeName = baseTypeName(String.valueOf(resultSetMetaData.get(position + 1).get("typeName")));
            if (!APPENDER_TYPES.contains(typeName)) {
                return "column [" + actual + "] has type " + typeName + ", which the appender does not bind";
            }
        }
        return null;
    }

    private static String physicalNames(List<Map<String, Object>> physical)
    {
        StringJoiner names = new StringJoiner(",");
        // skip the index 0 sentinel
        for (int position = 1; position < physical.size(); position++) {
            names.add(String.valueOf(physical.get(position).get("name")));
        }
        return "[" + names + "]";
    }

    /**
     * Splits the configured table name into catalog, schema and table segments, dropping the
     * quoting the configuration may carry because the appender takes bare identifiers.
     */
    private String[] unquotedTableParts()
    {
        return Arrays.stream(dataBaseType.unQuote(table).split("\\."))
                .map(dataBaseType::unQuote)
                .map(String::trim)
                .toArray(String[]::new);
    }

    private void appendColumn(DuckDBAppender target, int position, Column column)
            throws SQLException
    {
        if (column == null || column.getRawData() == null) {
            target.appendNull();
            return;
        }

        String typeName = baseTypeName(String.valueOf(resultSetMetaData.get(position + 1).get("typeName")));
        switch (typeName) {
            case "BOOLEAN":
                target.append(column.asBoolean());
                break;
            case "TINYINT":
                target.append(column.asLong().byteValue());
                break;
            case "SMALLINT":
                target.append(column.asLong().shortValue());
                break;
            case "INTEGER":
                target.append(column.asLong().intValue());
                break;
            case "BIGINT":
                target.append(column.asLong().longValue());
                break;
            case "FLOAT":
                target.append(column.asDouble().floatValue());
                break;
            case "DOUBLE":
                target.append(column.asDouble());
                break;
            case "DECIMAL":
                target.append(column.asBigDecimal());
                break;
            case "VARCHAR":
                target.append(column.asString());
                break;
            case "DATE":
                target.append(toLocalDate(column));
                break;
            case "TIME":
            case "TIME_NS":
                target.append(toLocalTime(column));
                break;
            case "TIMESTAMP":
            case "TIMESTAMP_S":
            case "TIMESTAMP_MS":
            case "TIMESTAMP_NS":
                target.append(column.asTimestamp().toLocalDateTime());
                break;
            default:
                // guarded by the whitelist check when the appender was opened
                throw new SQLException("Column [" + resultSetMetaData.get(position + 1).get("name")
                        + "] has type " + typeName + ", which the appender cannot bind");
        }
    }

    private static LocalDate toLocalDate(Column column)
    {
        return LocalDate.ofInstant(column.asDate().toInstant(), ZoneId.systemDefault());
    }

    private static LocalTime toLocalTime(Column column)
    {
        LocalTime value = LocalTime.ofInstant(column.asDate().toInstant(), ZoneId.systemDefault());
        if (column instanceof DateColumn dateColumn) {
            // the shared writer keeps sub-second digits this way, so match it to stay consistent
            value = value.withNano((int) dateColumn.getNanos());
        }
        return value;
    }

    /**
     * Reduces a DuckDB type name to its base name, mirroring the reader side.
     */
    private static String baseTypeName(String typeName)
    {
        if (typeName == null) {
            return "";
        }
        if (typeName.endsWith("[]")) {
            return "LIST";
        }
        if (typeName.endsWith("]")) {
            return "ARRAY";
        }
        int parenthesis = typeName.indexOf('(');
        return parenthesis < 0 ? typeName.trim() : typeName.substring(0, parenthesis).trim();
    }

    private void closeAppender()
    {
        if (appender != null) {
            try {
                // close flushes whatever the appender still buffers
                appender.close();
            }
            catch (Exception e) {
                LOG.warn("Failed to close the DuckDB appender: {}", e.getMessage());
            }
            appender = null;
        }
    }
}
