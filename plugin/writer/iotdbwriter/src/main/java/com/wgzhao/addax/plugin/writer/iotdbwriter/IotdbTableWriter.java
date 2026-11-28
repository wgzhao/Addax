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

package com.wgzhao.addax.plugin.writer.iotdbwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

/** Writes records to the IoTDB table model through JDBC batches. */
final class IotdbTableWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(IotdbTableWriter.class);

    private final IotdbWriterConfig config;
    private final String insertSql;
    private int[] targetTypes;

    IotdbTableWriter(IotdbWriterConfig config)
    {
        this.config = config;
        this.insertSql = "INSERT INTO " + config.table() + " (" + String.join(",", config.columns())
                + ") VALUES (" + String.join(",", java.util.Collections.nCopies(config.columns().size(), "?")) + ")";
    }

    void write(RecordReceiver receiver, TaskPluginCollector collector)
    {
        long records = 0;
        try (Connection connection = DriverManager.getConnection(config.jdbcUrl(), config.username(), config.password())) {
            initMetadata(connection);
            try (PreparedStatement statement = connection.prepareStatement(this.insertSql)) {
                List<Record> buffer = new ArrayList<>(config.batchSize());
                Record record;
                while ((record = receiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != config.columns().size()) {
                        collector.collectDirtyRecord(record,
                                "The record holds " + record.getColumnNumber() + " column(s), but the writer expects "
                                        + config.columns().size() + ".");
                        continue;
                    }
                    buffer.add(record);
                    records++;
                    if (buffer.size() >= config.batchSize()) {
                        flush(connection, statement, buffer, collector);
                    }
                }
                if (!buffer.isEmpty()) {
                    flush(connection, statement, buffer, collector);
                }
            }
        }
        catch (AddaxException e) {
            throw e;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "Failed to write records to IoTDB table " + config.table(), e);
        }
        LOG.info("Finished writing {} record(s) to IoTDB table {}.", records, config.table());
    }

    private void initMetadata(Connection connection)
    {
        String metadataSql = "SELECT " + String.join(",", config.columns()) + " FROM " + config.table()
                + " WHERE 1 = 2";
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(metadataSql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            if (metaData.getColumnCount() != config.columns().size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The table " + config.table() + " returns " + metaData.getColumnCount()
                                + " column(s), but " + config.columns().size() + " were configured.");
            }
            this.targetTypes = new int[config.columns().size()];
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                this.targetTypes[i - 1] = metaData.getColumnType(i);
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "Failed to read the metadata of IoTDB table " + config.table()
                            + ". Create the table first (or with preSql) and make sure it uses the 2.x table model.", e);
        }
    }

    private void flush(Connection connection, PreparedStatement statement, List<Record> buffer,
            TaskPluginCollector collector)
    {
        try {
            for (Record record : buffer) {
                bind(statement, record);
                statement.addBatch();
            }
            statement.executeBatch();
            statement.clearBatch();
        }
        catch (BatchUpdateException e) {
            LOG.warn("The batch write to {} failed, retrying {} record(s) individually: {}",
                    config.table(), buffer.size(), e.getMessage());
            retryIndividually(connection, buffer, collector);
        }
        catch (Exception e) {
            LOG.warn("The batch write to {} failed, retrying {} record(s) individually: {}",
                    config.table(), buffer.size(), e.getMessage());
            retryIndividually(connection, buffer, collector);
        }
        buffer.clear();
    }

    private void retryIndividually(Connection connection, List<Record> buffer, TaskPluginCollector collector)
    {
        try (PreparedStatement statement = connection.prepareStatement(this.insertSql)) {
            for (Record record : buffer) {
                try {
                    bind(statement, record);
                    statement.executeUpdate();
                }
                catch (Exception e) {
                    collector.collectDirtyRecord(record, e);
                }
            }
        }
        catch (SQLException e) {
            for (Record record : buffer) {
                collector.collectDirtyRecord(record, e);
            }
        }
    }

    private void bind(PreparedStatement statement, Record record)
            throws SQLException
    {
        for (int i = 0; i < config.columns().size(); i++) {
            if (i == config.timeColumnPosition()) {
                statement.setTimestamp(i + 1, IotdbWriterUtil.extractTimestamp(record, i));
                continue;
            }
            Column column = record.getColumn(i);
            if (column == null || column.getRawData() == null) {
                statement.setObject(i + 1, null);
                continue;
            }
            switch (this.targetTypes[i]) {
                case Types.BOOLEAN:
                case Types.BIT:
                    statement.setBoolean(i + 1, column.asBoolean());
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    statement.setLong(i + 1, column.asLong());
                    break;
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                    statement.setDouble(i + 1, column.asDouble());
                    break;
                case Types.DATE:
                    statement.setDate(i + 1, new java.sql.Date(column.asDate().getTime()));
                    break;
                case Types.TIME:
                    statement.setTime(i + 1, new java.sql.Time(column.asDate().getTime()));
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    statement.setTimestamp(i + 1, column.asTimestamp());
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    statement.setBytes(i + 1, column.asBytes());
                    break;
                default:
                    statement.setString(i + 1, column.asString());
                    break;
            }
        }
    }
}
