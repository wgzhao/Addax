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

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Shared helpers for the IoTDB reader. */
final class IotdbReaderUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(IotdbReaderUtil.class);

    static final String DRIVER = "org.apache.iotdb.jdbc.IoTDBDriver";
    static final String TREE = "tree";
    static final String TABLE = "table";

    private static final Pattern INTERVAL_PATTERN = Pattern.compile("^(\\d+)(ms|[dhms])$", Pattern.CASE_INSENSITIVE);
    private static final long SECOND = 1000L;
    private static final long MINUTE = 60L * SECOND;
    private static final long HOUR = 60L * MINUTE;
    private static final long DAY = 24L * HOUR;

    private IotdbReaderUtil()
    {
    }

    /**
     * IoTDB's JDBC jar has no META-INF/services/java.sql.Driver entry, so DriverManager
     * cannot discover it through the plugin class loader. Class.forName triggers the
     * driver's static DriverManager.registerDriver call; the TDengine and HBase SQL
     * plugins load their drivers explicitly for the same reason.
     */
    static void loadDriver()
    {
        try {
            Class.forName(DRIVER);
        }
        catch (ClassNotFoundException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The IoTDB JDBC driver was not found. Check that iotdb-jdbc is present in the plugin libs directory.", e);
        }
    }

    /**
     * Addax accepts both a single connection object and a one-element connection array.
     * This plugin talks to one cluster per job, so more than one entry is rejected.
     */
    static Configuration connectionConfig(Configuration conf)
    {
        Object raw = conf.get(Key.CONNECTION);
        if (raw == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [connection] is required.");
        }
        if (raw instanceof List<?>) {
            List<Configuration> connections = conf.getListConfiguration(Key.CONNECTION);
            if (connections.size() > 1) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The IoTDB reader supports exactly one connection, but " + connections.size() + " were configured.");
            }
            return connections.get(0);
        }
        Configuration connection = conf.getConfiguration(Key.CONNECTION);
        if (connection == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [connection] is required.");
        }
        return connection;
    }

    static String jdbcUrl(Configuration connection)
    {
        Object raw = connection.get(Key.JDBC_URL);
        if (raw == null) {
            return null;
        }
        if (raw instanceof List<?>) {
            List<String> urls = connection.getList(Key.JDBC_URL, String.class);
            return urls.isEmpty() ? null : StringUtils.trimToNull(urls.get(0));
        }
        return StringUtils.trimToNull(String.valueOf(raw));
    }

    /** Accepts both a scalar and an array for keys that may hold one or many strings. */
    static List<String> stringList(Configuration conf, String key)
    {
        Object raw = conf.get(key);
        if (raw == null) {
            return new ArrayList<>();
        }
        if (raw instanceof List<?>) {
            return conf.getList(key, String.class);
        }
        String value = StringUtils.trimToNull(String.valueOf(raw));
        List<String> result = new ArrayList<>();
        if (value != null) {
            result.add(value);
        }
        return result;
    }

    static boolean isTable(String dialect)
    {
        return TABLE.equalsIgnoreCase(dialect);
    }

    static String resolveDialect(Configuration conf, String jdbcUrl)
    {
        String dialect = StringUtils.trimToNull(conf.getString(IotdbKey.SQL_DIALECT));
        if (dialect == null) {
            String lower = jdbcUrl.toLowerCase(Locale.ROOT);
            dialect = lower.contains("sql_dialect=table") ? TABLE : TREE;
        }
        if (!TREE.equalsIgnoreCase(dialect) && !TABLE.equalsIgnoreCase(dialect)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The parameter [" + IotdbKey.SQL_DIALECT + "] must be tree or table, but was: " + dialect);
        }
        return dialect.toLowerCase(Locale.ROOT);
    }

    /** Table SQL is only understood by a 2.x server; make the dialect explicit in the URL. */
    static String appendDialect(String jdbcUrl, String dialect)
    {
        String normalized = normalizeJdbcUrl(jdbcUrl);
        if (!isTable(dialect)) {
            return normalized;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.contains("sql_dialect=")) {
            if (lower.contains("sql_dialect=table")) {
                return normalized;
            }
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The jdbcUrl sets a sql_dialect that conflicts with sqlDialect=table: " + jdbcUrl);
        }
        return normalized + (normalized.contains("?") ? "&" : "?") + "sql_dialect=table";
    }

    /**
     * The IoTDB driver treats the path of jdbc:iotdb://host:port/ as the default database.
     * With no database the slash has to be dropped before a query string is appended, otherwise
     * the driver reads "?sql_dialect=table" as the database name.
     */
    private static String normalizeJdbcUrl(String jdbcUrl)
    {
        try {
            java.net.URI uri = java.net.URI.create(jdbcUrl.substring("jdbc:".length()));
            if (uri.getRawAuthority() == null) {
                return jdbcUrl;
            }
            StringBuilder normalized = new StringBuilder("jdbc:iotdb://").append(uri.getRawAuthority());
            String path = uri.getRawPath();
            if (path != null && !path.isEmpty() && !"/".equals(path)) {
                normalized.append(path);
            }
            if (uri.getRawQuery() != null) {
                normalized.append('?').append(uri.getRawQuery());
            }
            return normalized.toString();
        }
        catch (RuntimeException e) {
            return jdbcUrl;
        }
    }

    static String stripSemicolon(String sql)
    {
        String value = StringUtils.trimToNull(sql);
        if (value == null) {
            return null;
        }
        while (value.endsWith(";")) {
            value = value.substring(0, value.length() - 1).trim();
        }
        return value;
    }

    /**
     * Parses the optional begin/end/interval triple into inclusive/exclusive epoch-millisecond ranges.
     *
     * @return empty when no time split is configured
     */
    static List<long[]> parseRanges(Configuration conf)
    {
        String begin = StringUtils.trimToNull(conf.getString(IotdbKey.BEGIN_DATETIME));
        String end = StringUtils.trimToNull(conf.getString(IotdbKey.END_DATETIME));
        String interval = StringUtils.trimToNull(conf.getString(IotdbKey.SPLIT_INTERVAL));
        List<long[]> ranges = new ArrayList<>();
        if (begin == null && end == null && interval == null) {
            return ranges;
        }
        if (begin == null || end == null || interval == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE,
                    "The parameters [" + IotdbKey.BEGIN_DATETIME + "], [" + IotdbKey.END_DATETIME
                            + "] and [" + IotdbKey.SPLIT_INTERVAL + "] must be configured together.");
        }

        long start = parseDateTime(begin, IotdbKey.BEGIN_DATETIME);
        long stop = parseDateTime(end, IotdbKey.END_DATETIME);
        long step = parseInterval(interval);
        if (start >= stop) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The parameter [" + IotdbKey.BEGIN_DATETIME + "] must be earlier than ["
                            + IotdbKey.END_DATETIME + "].");
        }
        for (long sliceStart = start; sliceStart < stop; ) {
            long sliceEnd = sliceStart + step;
            if (sliceEnd <= sliceStart || sliceEnd > stop) {
                sliceEnd = stop;
            }
            ranges.add(new long[]{sliceStart, sliceEnd});
            sliceStart = sliceEnd;
        }
        return ranges;
    }

    private static long parseDateTime(String value, String key)
    {
        if (value.matches("\\d+")) {
            return Long.parseLong(value);
        }
        SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATE_FORMAT);
        format.setLenient(false);
        try {
            return format.parse(value).getTime();
        }
        catch (ParseException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The parameter [" + key + "] must use the format " + Constant.DEFAULT_DATE_FORMAT
                            + " or epoch milliseconds, but was: " + value);
        }
    }

    private static long parseInterval(String value)
    {
        Matcher matcher = INTERVAL_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The parameter [" + IotdbKey.SPLIT_INTERVAL + "] must look like 1d, 6h, 30m, 10s or 500ms, but was: " + value);
        }
        long amount = Long.parseLong(matcher.group(1));
        if (amount <= 0) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The parameter [" + IotdbKey.SPLIT_INTERVAL + "] must be greater than zero.");
        }
        return switch (matcher.group(2).toLowerCase(Locale.ROOT)) {
            case "ms" -> amount;
            case "s" -> amount * SECOND;
            case "m" -> amount * MINUTE;
            case "h" -> amount * HOUR;
            case "d" -> amount * DAY;
            default -> throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Unsupported split interval: " + value);
        };
    }

    static String buildQuery(String select, String from, String where, long[] range)
    {
        StringBuilder sql = new StringBuilder("SELECT ").append(select).append(" FROM ").append(from);
        List<String> conditions = new ArrayList<>();
        if (StringUtils.isNotBlank(where)) {
            conditions.add("(" + where + ")");
        }
        if (range != null) {
            conditions.add("time >= " + range[0]);
            conditions.add("time < " + range[1]);
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        return sql.toString();
    }

    /**
     * Converts a JDBC cell to an Addax column. The single-argument getTimestamp is used on
     * purpose: IoTDB implements it, while the Calendar overload throws "Method not supported".
     */
    static Column convertColumn(ResultSet rs, ResultSetMetaData metaData, int index)
            throws SQLException
    {
        int type = metaData.getColumnType(index);
        switch (type) {
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return new TimestampColumn(rs.getTimestamp(index));
            case Types.DATE:
                return new DateColumn(rs.getDate(index));
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                Time time = rs.getTime(index);
                return new DateColumn(time);
            case Types.BOOLEAN:
            case Types.BIT:
                boolean bool = rs.getBoolean(index);
                return rs.wasNull() ? new BoolColumn((Boolean) null) : new BoolColumn(bool);
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                long longValue = rs.getLong(index);
                return rs.wasNull() ? new LongColumn((Long) null) : new LongColumn(longValue);
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                double doubleValue = rs.getDouble(index);
                return rs.wasNull() ? new DoubleColumn((Double) null) : new DoubleColumn(doubleValue);
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return new StringColumn(rs.getString(index));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return new BytesColumn(rs.getBytes(index));
            default:
                LOG.debug("Unknown IoTDB JDBC type {} ({}), falling back to getString", type,
                        metaData.getColumnTypeName(index));
                return new StringColumn(rs.getString(index));
        }
    }
}
