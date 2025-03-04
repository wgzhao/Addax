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

package com.wgzhao.addax.plugin.writer.hbase11xsqlwriter;

import com.wgzhao.addax.common.base.HBaseConstant;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
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
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

/**
 * @author yanghan.y
 */
public class HbaseSQLWriterTask
{
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSQLWriterTask.class);
    private final HbaseSQLWriterConfig cfg;
    private TaskPluginCollector taskPluginCollector;
    private Connection connection = null;
    private PreparedStatement ps = null;
    private int numberOfColumnsToWrite;
    private int numberOfColumnsToRead;
    private int[] columnTypes;

    public HbaseSQLWriterTask(Configuration configuration)
    {
        cfg = HbaseSQLHelper.parseConfig(configuration);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector)
    {
        this.taskPluginCollector = taskPluginCollector;
        Record record;
        try {
            prepare();

            List<Record> buffer = new ArrayList<>(cfg.getBatchSize());
            while ((record = lineReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != numberOfColumnsToRead) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The number of fields(" + record.getColumnNumber()
                                    + ") in the source and the number of fields(" + numberOfColumnsToRead + ") you configured in 'column' are not the same.");
                }

                buffer.add(record);
                if (buffer.size() > cfg.getBatchSize()) {
                    doBatchUpsert(buffer);
                    buffer.clear();
                }
            } // end while loop

            if (!buffer.isEmpty()) {
                doBatchUpsert(buffer);
                buffer.clear();
            }
        }
        catch (Throwable t) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, t);
        }
        finally {
            close();
        }
    }

    private void prepare()
            throws SQLException
    {
        if (connection == null) {
            connection = HbaseSQLHelper.getJdbcConnection(cfg);
            connection.setAutoCommit(false);    // 批量提交
        }

        if (ps == null) {
            ps = createPreparedStatement();
            columnTypes = getColumnSqlType(cfg.getColumns());
        }
    }

    private void close()
    {
        if (ps != null) {
            try {
                ps.close();
            }
            catch (SQLException e) {
                LOG.error("Failed to close PreparedStatement", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                LOG.error("Failed to close Connection", e);
            }
        }
    }

    private void doBatchUpsert(List<Record> records)
            throws SQLException
    {
        try {
            for (Record r : records) {
                setupStatement(r);
                ps.executeUpdate();
            }

            // 将缓存的数据提交到hbase
            connection.commit();
        }
        catch (SQLException e) {
            LOG.error("Failed to batch commit {} records", records.size(), e);

            connection.rollback();
            doSingleUpsert(records);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
        }
    }

    private void doSingleUpsert(List<Record> records)
    {
        for (Record r : records) {
            try {
                setupStatement(r);
                ps.executeUpdate();
                connection.commit();
            }
            catch (SQLException e) {
                LOG.error("Failed to write hbase", e);
                this.taskPluginCollector.collectDirtyRecord(r, e);
            }
        }
    }

    private PreparedStatement createPreparedStatement()
            throws SQLException
    {
        String columnNames = String.join(",", cfg.getColumns());

        numberOfColumnsToWrite = cfg.getColumns().size();
        numberOfColumnsToRead = numberOfColumnsToWrite;   // 开始的时候，要读的列数娱要写的列数相等

        String tableName = cfg.getTableName();
        String sql = String.format("UPSERT INTO %s ( %s ) VALUES ( %s )", tableName, columnNames, String.join(",", Collections.nCopies(cfg.getColumns().size(), "?")));
        PreparedStatement ps = connection.prepareStatement(sql);
        LOG.debug("SQL template generated: {}", sql);
        return ps;
    }

    private int[] getColumnSqlType(List<String> columnNames)
            throws SQLException
    {
        int[] types = new int[numberOfColumnsToWrite];
        PTable ptable = HbaseSQLHelper
                .getTableSchema(connection, cfg.getNamespace(), cfg.getTableName(), cfg.isThinClient());

        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            PDataType type = ptable.getColumnForColumnName(name).getDataType();
            types[i] = type.getSqlType();
            LOG.debug("Column name : {}, sql type = {} {} ", name, type.getSqlType(), type.getSqlTypeName());
        }
        return types;
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
                    ps.setString(pos, col.asString());
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                    ps.setBytes(pos, col.asBytes());
                    break;

                case Types.BOOLEAN:
                    ps.setBoolean(pos, col.asBoolean());
                    break;

                case Types.TINYINT:
                case HBaseConstant.TYPE_UNSIGNED_TINYINT:
                    ps.setByte(pos, col.asLong().byteValue());
                    break;

                case Types.SMALLINT:
                case HBaseConstant.TYPE_UNSIGNED_SMALLINT:
                    ps.setShort(pos, col.asLong().shortValue());
                    break;

                case Types.INTEGER:
                case HBaseConstant.TYPE_UNSIGNED_INTEGER:
                    ps.setInt(pos, col.asLong().intValue());
                    break;

                case Types.BIGINT:
                case HBaseConstant.TYPE_UNSIGNED_LONG:
                    ps.setLong(pos, col.asLong());
                    break;

                case Types.FLOAT:
                    ps.setFloat(pos, col.asDouble().floatValue());
                    break;

                case Types.DOUBLE:
                    ps.setDouble(pos, col.asDouble());
                    break;

                case Types.DECIMAL:
                    ps.setBigDecimal(pos, col.asBigDecimal());
                    break;

                case Types.DATE:
                case HBaseConstant.TYPE_UNSIGNED_DATE:
                    ps.setDate(pos, new java.sql.Date(col.asDate().getTime()));
                    break;

                case Types.TIME:
                case HBaseConstant.TYPE_UNSIGNED_TIME:
                    ps.setTime(pos, new java.sql.Time(col.asDate().getTime()));
                    break;

                case Types.TIMESTAMP:
                case HBaseConstant.TYPE_UNSIGNED_TIMESTAMP:
                    ps.setTimestamp(pos, new java.sql.Timestamp(col.asDate().getTime()));
                    break;

                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The data type " + sqlType + "is unsupported.");
            } // end switch
        }
        else {
            switch (cfg.getNullMode()) {
                case SKIP:
                    ps.setNull(pos, sqlType);
                    break;

                case EMPTY:
                    ps.setObject(pos, getEmptyValue(sqlType));
                    break;

                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The nullMode type " + cfg.getNullMode() + " is unsupported, here are available nullMode:" + Arrays.asList(NullModeType.values()));
            }
        }
    }

    /**
     * Get the empty value for the specified SQL type.
     *
     * @param sqlType the SQL type
     * @return the empty value
     */
    private Object getEmptyValue(int sqlType)
    {
        switch (sqlType) {
            case Types.CHAR:
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
                return new java.sql.Date(0);

            case Types.TIME:
            case HBaseConstant.TYPE_UNSIGNED_TIME:
                return new java.sql.Time(0);

            case Types.TIMESTAMP:
            case HBaseConstant.TYPE_UNSIGNED_TIMESTAMP:
                return new java.sql.Timestamp(0);

            case Types.BINARY:
            case Types.VARBINARY:
                return new byte[0];

            default:
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The data type " + sqlType + " is unsupported yet.");
        }
    }
}
