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

package com.wgzhao.addax.plugin.reader.iotdbreader;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Apache IoTDB reader. */
public class IotdbReader
        extends Reader
{
    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;
        private String jdbcUrl;
        private String username;
        private String password;
        private String dialect;
        private String where;
        private String device;
        private List<String> measurements;
        private String table;
        private List<String> columns;
        private List<String> querySqls;
        private List<long[]> ranges;
        private int timeColumnPosition;
        private int fetchSize;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            this.username = StringUtils.trimToNull(this.originalConfig.getString(Key.USERNAME));
            if (this.username == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [username] is required.");
            }
            this.password = this.originalConfig.getString(Key.PASSWORD, "");

            Configuration connection = IotdbReaderUtil.connectionConfig(this.originalConfig);
            this.jdbcUrl = IotdbReaderUtil.jdbcUrl(connection);
            if (this.jdbcUrl == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [connection.jdbcUrl] is required.");
            }
            this.dialect = IotdbReaderUtil.resolveDialect(this.originalConfig, this.jdbcUrl);
            this.jdbcUrl = IotdbReaderUtil.appendDialect(this.jdbcUrl, this.dialect);

            this.fetchSize = this.originalConfig.getInt(Key.FETCH_SIZE, 10000);
            if (this.fetchSize <= 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The parameter [fetchSize] must be greater than zero.");
            }
            this.timeColumnPosition = this.originalConfig.getInt(IotdbKey.TIME_COLUMN_POSITION, 0);
            if (this.timeColumnPosition < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The parameter [" + IotdbKey.TIME_COLUMN_POSITION + "] must not be negative.");
            }
            this.where = IotdbReaderUtil.stripSemicolon(this.originalConfig.getString(Key.WHERE));

            this.querySqls = configuredQuerySqls(this.originalConfig, connection);
            this.ranges = IotdbReaderUtil.parseRanges(this.originalConfig);

            if (IotdbReaderUtil.isTable(this.dialect)) {
                IotdbReaderUtil.loadDriver();
                verifyTableDialect();
                if (this.querySqls.isEmpty()) {
                    this.table = singleTable(this.originalConfig, connection);
                    List<String> configuredColumns = IotdbReaderUtil.stringList(this.originalConfig, Key.COLUMN);
                    if (configuredColumns.isEmpty()) {
                        configuredColumns = IotdbReaderUtil.stringList(connection, Key.COLUMN);
                    }
                    this.columns = configuredColumns.isEmpty() ? List.of("*") : configuredColumns;
                    if (this.columns.size() > 1 && this.columns.contains("*")) {
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                "The wildcard '*' cannot be combined with other columns.");
                    }
                }
            }
            else if (this.querySqls.isEmpty()) {
                this.device = StringUtils.trimToNull(this.originalConfig.getString(IotdbKey.DEVICE));
                if (this.device == null) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE,
                            "The parameter [" + IotdbKey.DEVICE + "] is required when querySqls is empty.");
                }
                this.measurements = IotdbReaderUtil.stringList(this.originalConfig, IotdbKey.MEASUREMENTS);
                if (this.measurements.isEmpty()) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE,
                            "The parameter [" + IotdbKey.MEASUREMENTS + "] is required when querySqls is empty.");
                }
                if (this.measurements.size() > 1 && this.measurements.contains("*")) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The wildcard '*' cannot be combined with other measurements.");
                }
                if (!"*".equals(this.measurements.get(0))
                        && this.timeColumnPosition > this.measurements.size()) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The parameter [" + IotdbKey.TIME_COLUMN_POSITION + "] is "
                                    + this.timeColumnPosition + " but only " + this.measurements.size()
                                    + " measurement column(s) are configured.");
                }
            }

            if (!this.querySqls.isEmpty() && !this.ranges.isEmpty()) {
                LOG.warn("querySqls is configured, the time range split parameters are ignored.");
                this.ranges = new ArrayList<>();
            }
            LOG.info("IoTDB reader initialized, dialect={}, fetchSize={}, time ranges={}, querySqls={}.",
                    this.dialect, this.fetchSize, this.ranges.size(), this.querySqls.size());
        }

        private static List<String> configuredQuerySqls(Configuration conf, Configuration connection)
        {
            List<String> sqls = IotdbReaderUtil.stringList(conf, IotdbKey.QUERY_SQLS);
            if (sqls.isEmpty()) {
                sqls = IotdbReaderUtil.stringList(conf, Key.QUERY_SQL);
            }
            // connection.querySql is the shape used by the RDBMS readers; accept it as well
            if (sqls.isEmpty()) {
                sqls = IotdbReaderUtil.stringList(connection, IotdbKey.QUERY_SQLS);
            }
            if (sqls.isEmpty()) {
                sqls = IotdbReaderUtil.stringList(connection, Key.QUERY_SQL);
            }
            List<String> result = new ArrayList<>();
            for (String sql : sqls) {
                if (StringUtils.isNotBlank(sql)) {
                    result.add(sql.trim());
                }
            }
            return result;
        }

        private static String singleTable(Configuration conf, Configuration connection)
        {
            List<String> tables = IotdbReaderUtil.stringList(conf, Key.TABLE);
            if (tables.isEmpty()) {
                tables = IotdbReaderUtil.stringList(connection, Key.TABLE);
            }
            String table;
            if (!tables.isEmpty()) {
                if (tables.size() > 1) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The IoTDB reader supports one table per job, but " + tables.size() + " were configured.");
                }
                table = StringUtils.trimToNull(tables.get(0));
            }
            else {
                table = StringUtils.trimToNull(conf.getString(Key.TABLE));
            }
            if (table == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [table] is required in table mode.");
            }
            return table;
        }

        private void verifyTableDialect()
        {
            // SELECT 1 is valid in the table dialect and fails to parse on the tree-only 1.x line
            try (Connection connection = DriverManager.getConnection(this.jdbcUrl, this.username, this.password);
                    Statement statement = connection.createStatement();
                    ResultSet ignored = statement.executeQuery("SELECT 1")) {
                LOG.debug("Verified that the target supports the IoTDB table model.");
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The IoTDB table model requires a 2.x server (SELECT 1 failed: " + e.getMessage()
                                + "). Check the jdbcUrl, credentials and server version.", e);
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> slices = new ArrayList<>();
            if (!this.querySqls.isEmpty()) {
                for (String sql : this.querySqls) {
                    slices.add(newSlice(sql));
                }
                return slices;
            }

            if (this.ranges.isEmpty()) {
                slices.add(newSlice(buildQuery(null)));
                return slices;
            }

            for (long[] range : this.ranges) {
                slices.add(newSlice(buildQuery(range)));
            }
            return slices;
        }

        private Configuration newSlice(String querySql)
        {
            Configuration slice = this.originalConfig.clone();
            slice.set(Key.JDBC_URL, this.jdbcUrl);
            if (slice.get(Key.CONNECTION) != null) {
                slice.remove(Key.CONNECTION);
            }
            if (slice.get(IotdbKey.QUERY_SQLS) != null) {
                slice.remove(IotdbKey.QUERY_SQLS);
            }
            slice.set(Key.QUERY_SQL, querySql);
            slice.set(IotdbKey.SQL_DIALECT, this.dialect);
            slice.set(IotdbKey.TIME_COLUMN_POSITION, this.timeColumnPosition);
            slice.set(Key.FETCH_SIZE, this.fetchSize);
            slice.set(Key.USERNAME, this.username);
            slice.set(Key.PASSWORD, this.password);
            return slice;
        }

        private String buildQuery(long[] range)
        {
            if (IotdbReaderUtil.isTable(this.dialect)) {
                return IotdbReaderUtil.buildQuery(String.join(",", this.columns), this.table, this.where, range);
            }
            String select = this.measurements.size() == 1 && "*".equals(this.measurements.get(0))
                    ? "*" : String.join(",", this.measurements);
            return IotdbReaderUtil.buildQuery(select, this.device, this.where, range);
        }

        @Override
        public void post()
        {
            // no job-level resources
        }

        @Override
        public void destroy()
        {
            // no job-level resources
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String jdbcUrl;
        private String username;
        private String password;
        private String querySql;
        private String dialect;
        private int timeColumnPosition;
        private int fetchSize;

        @Override
        public void init()
        {
            Configuration conf = getPluginJobConf();
            this.jdbcUrl = StringUtils.trimToNull(conf.getString(Key.JDBC_URL));
            if (this.jdbcUrl == null) {
                Configuration connection = IotdbReaderUtil.connectionConfig(conf);
                String configuredUrl = IotdbReaderUtil.jdbcUrl(connection);
                if (configuredUrl == null) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [connection.jdbcUrl] is required.");
                }
                this.jdbcUrl = IotdbReaderUtil.appendDialect(configuredUrl,
                        IotdbReaderUtil.resolveDialect(conf, configuredUrl));
            }
            this.username = conf.getString(Key.USERNAME);
            this.password = conf.getString(Key.PASSWORD, "");
            this.querySql = StringUtils.trimToNull(conf.getString(Key.QUERY_SQL));
            if (this.querySql == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The SQL query for the reader task is empty.");
            }
            this.dialect = conf.getString(IotdbKey.SQL_DIALECT, IotdbReaderUtil.TREE);
            this.timeColumnPosition = conf.getInt(IotdbKey.TIME_COLUMN_POSITION, 0);
            this.fetchSize = conf.getInt(Key.FETCH_SIZE, 10000);
            IotdbReaderUtil.loadDriver();
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.info("Reading IoTDB with SQL: {}", this.querySql);
            long count = 0;
            try (Connection connection = DriverManager.getConnection(this.jdbcUrl, this.username, this.password);
                    Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                statement.setFetchSize(this.fetchSize);
                try (ResultSet rs = statement.executeQuery(this.querySql)) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    boolean tree = !IotdbReaderUtil.isTable(this.dialect);
                    // aggregate queries may not return the implicit time column
                    boolean hasTime = tree && columnCount > 0 && metaData.getColumnType(1) == Types.TIMESTAMP;
                    while (rs.next()) {
                        Record record = recordSender.createRecord();
                        try {
                            if (tree && hasTime) {
                                fillTreeRecord(record, rs, metaData, columnCount);
                            }
                            else {
                                fillRecord(record, rs, metaData, columnCount);
                            }
                        }
                        catch (Exception e) {
                            getTaskPluginCollector().collectDirtyRecord(record, e);
                            continue;
                        }
                        recordSender.sendToWriter(record);
                        count++;
                    }
                }
            }
            catch (SQLException e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to read from IoTDB with SQL: " + this.querySql, e);
            }
            LOG.info("Finished reading {} record(s) from IoTDB.", count);
        }

        private void fillTreeRecord(Record record, ResultSet rs, ResultSetMetaData metaData, int columnCount)
                throws SQLException
        {
            Column time = IotdbReaderUtil.convertColumn(rs, metaData, 1);
            List<Column> values = new ArrayList<>(columnCount);
            for (int i = 2; i <= columnCount; i++) {
                values.add(IotdbReaderUtil.convertColumn(rs, metaData, i));
            }
            if (this.timeColumnPosition > values.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The parameter [" + IotdbKey.TIME_COLUMN_POSITION + "] is " + this.timeColumnPosition
                                + " but the result has only " + values.size() + " measurement column(s).");
            }
            values.add(this.timeColumnPosition, time);
            for (Column column : values) {
                record.addColumn(column);
            }
        }

        private void fillRecord(Record record, ResultSet rs, ResultSetMetaData metaData, int columnCount)
                throws SQLException
        {
            for (int i = 1; i <= columnCount; i++) {
                record.addColumn(IotdbReaderUtil.convertColumn(rs, metaData, i));
            }
        }

        @Override
        public void post()
        {
            // no task-level resources
        }

        @Override
        public void destroy()
        {
            // no task-level resources
        }
    }
}
