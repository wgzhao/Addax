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

package com.wgzhao.addax.plugin.writer.mysqlwriter;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.ErrorCode;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class MysqlWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck()
        {
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterTask;

        @Override
        public void init()
        {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE)
            {

                @Override
                protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                        int columnSqlType, Column column)
                        throws SQLException
                {
                    if (column == null || column.getRawData() == null) {
                        preparedStatement.setObject(columnIndex, null);
                        return preparedStatement;
                    }
                    if (columnSqlType == Types.BIT) {
                        // BIT(1) -> java.lang.Boolean
                        if (column.getType() == Column.Type.BOOL) {
                            preparedStatement.setBoolean(columnIndex, column.asBoolean());
                        }
                        else {
                            // BIT ( > 1) -> byte[]
                            preparedStatement.setObject(columnIndex, Integer.valueOf(column.asString(), 2));
                        }
                        return preparedStatement;
                    }
                    if (columnSqlType == Types.DATE && "YEAR".equals(this.resultSetMetaData.get(columnIndex).get("typeName"))) {
                        preparedStatement.setLong(columnIndex, column.asLong());
                        return preparedStatement;
                    }
                    if (columnSqlType == Types.BINARY && "GEOMETRY".equals(this.resultSetMetaData.get(columnIndex).get("typeName"))) {
                        // GEOMETRY type is not supported by MySQL JDBC driver, so we convert it to String
                        // get the srid value
                        int srid = 0;
                        String schema;
                        String tableName;
                        if (this.table.contains(".")) {
                            schema = "'" + this.table.split("\\.")[0].trim() + "'";
                            tableName = this.table.split("\\.")[1].trim();
                        } else {
                            schema = "schema()";
                            tableName = this.table;
                        }
                        Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, this.username, this.password);
                        String sql = String.format("""
                                        SELECT SRS_ID
                                        FROM INFORMATION_SCHEMA.ST_GEOMETRY_COLUMNS
                                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'
                                        """, schema, tableName, this.resultSetMetaData.get(columnIndex).get("name"));
                        ResultSet resultSet = connection.createStatement().executeQuery(sql);
                        if (resultSet.next()) {
                            srid = resultSet.getInt("SRS_ID");
                        }
                        Geometry geometry;
                        if (column.getType() == Column.Type.STRING) {
                            WKTReader wktReader = new WKTReader();
                            try {
                                geometry = wktReader.read(column.asString());
                            }
                            catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        else  {
                            // If it's not a String, we convert it to String
                            WKBReader wkbReader = new WKBReader();
                            try {
                                geometry = wkbReader.read(column.asBytes());
                            }
                            catch (ParseException e) {
                                throw AddaxException.asAddaxException(ErrorCode.RUNTIME_ERROR,
                                        String.format("Failed to parse WKB geometry: %s", e.getMessage()), e);
                            }
                        }
                        geometry.setSRID(srid);
                        byte[] wkb = new WKBWriter(2, 2, false).write(geometry);
                        ByteBuffer buffer = ByteBuffer.allocate(4 + wkb.length);
                        buffer.putInt(Integer.reverseBytes(srid)); // Write SRID in little-endian
                        buffer.put(wkb); // Write WKB data
                        preparedStatement.setBytes(columnIndex, buffer.array());
                        return preparedStatement;
                    }
                    return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqlType, column);
                }
            };
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver()
        {
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }
    }
}
