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

package com.wgzhao.addax.plugin.writer.hbase20xsqlwriter;

import com.wgzhao.addax.core.base.HBaseConstant;
import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class HBase20xSQLWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(HBase20xSQLWriterTask.class);

    private final Configuration configuration;
    private TaskPluginCollector taskPluginCollector;

    private Connection connection = null;
    private PreparedStatement pstmt = null;

    private int numberOfColumnsToWrite;
    private int numberOfColumnsToRead;
    private int[] columnTypes;
    private List<String> columns;
    private String fullTableName;

    private NullModeType nullModeType;
    private int batchSize;

    public HBase20xSQLWriterTask(Configuration configuration)
    {
        this.configuration = configuration;
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector)
    {
        this.taskPluginCollector = taskPluginCollector;

        try {
            initialize();

            writeData(lineReceiver);
        }
        catch (Throwable e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        finally {
            HBase20xSQLHelper.closeJdbc(connection, pstmt, null);
        }
    }

    private void initialize()
            throws SQLException
    {
        if (connection == null) {
            connection = HBase20xSQLHelper.getJdbcConnection(configuration);
            connection.setAutoCommit(false);
        }
        nullModeType = NullModeType.getByTypeName(configuration.getString(HBaseKey.NULL_MODE, HBaseConstant.DEFAULT_NULL_MODE));
        batchSize = configuration.getInt(HBaseKey.BATCH_SIZE, HBaseConstant.DEFAULT_BATCH_ROW_COUNT);
        String schema = configuration.getString(HBaseKey.SCHEMA);
        String tableName = configuration.getNecessaryValue(HBaseKey.TABLE, REQUIRED_VALUE);
        fullTableName = "\"" + tableName + "\"";
        if (schema != null && !schema.isEmpty()) {
            fullTableName = "\"" + schema + "\".\"" + tableName + "\"";
        }
        columns = configuration.getList(HBaseKey.COLUMN, String.class);
        if (pstmt == null) {
            pstmt = createPreparedStatement();
            columnTypes = getColumnSqlType();
        }
    }


    private PreparedStatement createPreparedStatement()
            throws SQLException
    {
        StringBuilder columnNamesBuilder = new StringBuilder();
        for (String col : columns) {
            columnNamesBuilder.append("\"");
            columnNamesBuilder.append(col);
            columnNamesBuilder.append("\"");
            columnNamesBuilder.append(",");
        }
        columnNamesBuilder.setLength(columnNamesBuilder.length() - 1);
        String columnNames = columnNamesBuilder.toString();
        numberOfColumnsToWrite = columns.size();
        numberOfColumnsToRead = numberOfColumnsToWrite;

        StringBuilder upsertBuilder =
                new StringBuilder("upsert into " + fullTableName + " (" + columnNames + " ) values (");
        for (int i = 0; i < numberOfColumnsToWrite; i++) {
            upsertBuilder.append("?,");
        }
        // remove the trailing comma
        upsertBuilder.setLength(upsertBuilder.length() - 1);
        upsertBuilder.append(")");

        String sql = upsertBuilder.toString();
        PreparedStatement ps = connection.prepareStatement(sql);
        LOG.debug("SQL template generated: {}", sql);
        return ps;
    }

    private int[] getColumnSqlType()
    {
        int[] types = new int[numberOfColumnsToWrite];
        StringBuilder columnNamesBuilder = new StringBuilder();
        for (String columnName : columns) {
            columnNamesBuilder.append("\"").append(columnName).append("\",");
        }
        columnNamesBuilder.setLength(columnNamesBuilder.length() - 1);
        String selectSql = "SELECT " + columnNamesBuilder + " FROM " + fullTableName + " LIMIT 1";
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSetMetaData meta = statement.executeQuery(selectSql).getMetaData();

            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i);
                types[i] = meta.getColumnType(i + 1);
                LOG.debug("Column name : {}, sql type = {} {}", name, types[i], meta.getColumnTypeName(i + 1));
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                   "Failed to get the columns of "+ fullTableName, e);
        }
        finally {
            HBase20xSQLHelper.closeJdbc(null, statement, null);
        }

        return types;
    }

    private void writeData(RecordReceiver lineReceiver)
            throws SQLException
    {
        List<Record> buffer = new ArrayList<>(batchSize);
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            if (record.getColumnNumber() != numberOfColumnsToRead) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The number of columns(" + record.getColumnNumber() +
                        ") is not equal to  the number of columns (" + numberOfColumnsToRead  + "in your configuration");
            }

            buffer.add(record);
            if (buffer.size() > batchSize) {
                doBatchUpsert(buffer);
                buffer.clear();
            }
        }

        if (!buffer.isEmpty()) {
            doBatchUpsert(buffer);
            buffer.clear();
        }
    }

    private void doBatchUpsert(List<Record> records)
            throws SQLException
    {
        try {
            for (Record r : records) {
                setupStatement(r);
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            connection.commit();
            pstmt.clearParameters();
            pstmt.clearBatch();
        }
        catch (SQLException e) {
            LOG.error("Failed batch committing {} records", records.size(), e);

            connection.rollback();
            HBase20xSQLHelper.closeJdbc(null, pstmt, null);
            connection.setAutoCommit(true);
            pstmt = createPreparedStatement();
            doSingleUpsert(records);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    private void doSingleUpsert(List<Record> records)
    {
        int rowNumber = 0;
        for (Record r : records) {
            try {
                rowNumber++;
                setupStatement(r);
                pstmt.executeUpdate();
            }
            catch (SQLException e) {
                LOG.error("Failed writing to phoenix, rowNumber: {}", rowNumber);
                this.taskPluginCollector.collectDirtyRecord(r, e);
            }
        }
    }

    private void setupStatement(Record record)
            throws SQLException
    {
        for (int i = 0; i < numberOfColumnsToWrite; i++) {
            Column col = record.getColumn(i);
            int sqlType = columnTypes[i];
            setupColumn(i + 1, sqlType, col);
        }
    }

    private void setupColumn(int pos, int sqlType, Column col)
            throws SQLException
    {
        if (col.getRawData() != null) {
            switch (sqlType) {
                case Types.CHAR:
                case Types.VARCHAR:
                    pstmt.setString(pos, col.asString());
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                    pstmt.setBytes(pos, col.asBytes());
                    break;

                case Types.BOOLEAN:
                    pstmt.setBoolean(pos, col.asBoolean());
                    break;

                case Types.TINYINT:
                case HBaseConstant.TYPE_UNSIGNED_TINYINT:
                    pstmt.setByte(pos, col.asLong().byteValue());
                    break;

                case Types.SMALLINT:
                case HBaseConstant.TYPE_UNSIGNED_SMALLINT:
                    pstmt.setShort(pos, col.asLong().shortValue());
                    break;

                case Types.INTEGER:
                case HBaseConstant.TYPE_UNSIGNED_INTEGER:
                    pstmt.setInt(pos, col.asLong().intValue());
                    break;

                case Types.BIGINT:
                case HBaseConstant.TYPE_UNSIGNED_LONG:
                    pstmt.setLong(pos, col.asLong());
                    break;

                case Types.FLOAT:
                    pstmt.setFloat(pos, col.asDouble().floatValue());
                    break;

                case Types.DOUBLE:
                    pstmt.setDouble(pos, col.asDouble());
                    break;

                case Types.DECIMAL:
                    pstmt.setBigDecimal(pos, col.asBigDecimal());
                    break;

                case Types.DATE:
                case HBaseConstant.TYPE_UNSIGNED_DATE:
                    pstmt.setDate(pos, new Date(col.asDate().getTime()));
                    break;

                case Types.TIME:
                case HBaseConstant.TYPE_UNSIGNED_TIME:
                    pstmt.setTime(pos, new Time(col.asDate().getTime()));
                    break;

                case Types.TIMESTAMP:
                case HBaseConstant.TYPE_UNSIGNED_TIMESTAMP:
                    pstmt.setTimestamp(pos, new Timestamp(col.asDate().getTime()));
                    break;

                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The column type " + sqlType + " is unsupported");
            }
        }
        else {
            switch (nullModeType) {
                case SKIP:
                    pstmt.setNull(pos, sqlType);
                    break;

                case EMPTY:
                    pstmt.setObject(pos, getEmptyValue(sqlType));
                    break;

                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The value of nullMode is not supported, it should be either skip or empty");
            }
        }
    }

    private Object getEmptyValue(int sqlType)
    {
        switch (sqlType) {
            case Types.VARCHAR:
                return "";

            case Types.BOOLEAN:
                return false;

            case Types.TINYINT:
            case HBaseConstant.TYPE_UNSIGNED_TINYINT:
                return (byte) 0;

            case Types.SMALLINT:
            case HBaseConstant.TYPE_UNSIGNED_SMALLINT:
                return (short) 0;

            case Types.INTEGER:
            case HBaseConstant.TYPE_UNSIGNED_INTEGER:
                return 0;

            case Types.BIGINT:
            case HBaseConstant.TYPE_UNSIGNED_LONG:
                return (long) 0;

            case Types.FLOAT:
                return (float) 0.0;

            case Types.DOUBLE:
                return 0.0;

            case Types.DECIMAL:
                return new BigDecimal(0);

            case Types.DATE:
            case HBaseConstant.TYPE_UNSIGNED_DATE:
                return new Date(0);

            case Types.TIME:
            case HBaseConstant.TYPE_UNSIGNED_TIME:
                return new Time(0);

            case Types.TIMESTAMP:
            case HBaseConstant.TYPE_UNSIGNED_TIMESTAMP:
                return new Timestamp(0);

            case Types.BINARY:
            case Types.VARBINARY:
                return new byte[0];

            default:
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The column type " + sqlType + " is unsupported");
        }
    }
}
