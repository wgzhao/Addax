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

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Immutable parsed writer configuration shared by the job and its tasks. */
final class IotdbWriterConfig
{
    private static final Logger LOG = LoggerFactory.getLogger(IotdbWriterConfig.class);
    private static final int DEFAULT_PORT = 6667;

    private final String jdbcUrl;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String dialect;
    private final boolean tree;
    private final String device;
    private final String table;
    private final List<String> measurements;
    private final List<String> columns;
    private final List<TSDataType> columnTypes;
    private final int timeColumnPosition;
    private final int batchSize;
    private final List<String> preSqls;
    private final List<String> postSqls;

    private IotdbWriterConfig(String jdbcUrl, String host, int port, String database, String username,
            String password, String dialect, boolean tree, String device, String table,
            List<String> measurements, List<String> columns, List<TSDataType> columnTypes,
            int timeColumnPosition, int batchSize, List<String> preSqls, List<String> postSqls)
    {
        this.jdbcUrl = jdbcUrl;
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.dialect = dialect;
        this.tree = tree;
        this.device = device;
        this.table = table;
        this.measurements = measurements;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.timeColumnPosition = timeColumnPosition;
        this.batchSize = batchSize;
        this.preSqls = preSqls;
        this.postSqls = postSqls;
    }

    static IotdbWriterConfig from(Configuration conf)
    {
        String username = StringUtils.trimToNull(conf.getString(Key.USERNAME));
        if (username == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [username] is required.");
        }
        String password = conf.getString(Key.PASSWORD, "");

        Configuration connection = IotdbWriterUtil.connectionConfig(conf);
        String jdbcUrl = IotdbWriterUtil.jdbcUrl(connection);
        if (jdbcUrl == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "The parameter [connection.jdbcUrl] is required.");
        }
        String dialect = IotdbWriterUtil.resolveDialect(conf, jdbcUrl);
        jdbcUrl = IotdbWriterUtil.appendDialect(jdbcUrl, dialect);
        boolean tree = !IotdbWriterUtil.isTable(dialect);

        Endpoint endpoint = parseEndpoint(jdbcUrl);

        String device = null;
        String table = null;
        List<String> measurements = List.of();
        List<String> columns = List.of();
        List<TSDataType> columnTypes = List.of();
        int expectedColumns;
        if (tree) {
            device = StringUtils.trimToNull(conf.getString(IotdbKey.DEVICE));
            if (device == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [" + IotdbKey.DEVICE + "] is required in tree mode.");
            }
            measurements = IotdbWriterUtil.stringList(conf, IotdbKey.MEASUREMENTS);
            if (measurements.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [" + IotdbKey.MEASUREMENTS + "] is required in tree mode.");
            }
            if (measurements.contains("*")) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The wildcard '*' cannot be used as a writer measurement.");
            }
            expectedColumns = measurements.size() + 1;

            List<String> configuredTypes = IotdbWriterUtil.nonBlank(
                    IotdbWriterUtil.stringList(conf, IotdbKey.COLUMN_TYPE));
            if (!configuredTypes.isEmpty()) {
                if (configuredTypes.size() != measurements.size()) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The parameter [" + IotdbKey.COLUMN_TYPE + "] has " + configuredTypes.size()
                                    + " type(s), but " + measurements.size() + " measurement(s) were configured.");
                }
                columnTypes = parseTypes(configuredTypes);
            }
        }
        else {
            table = IotdbWriterUtil.singleTable(conf, connection);
            columns = IotdbWriterUtil.stringList(conf, Key.COLUMN);
            if (columns.isEmpty()) {
                columns = IotdbWriterUtil.stringList(connection, Key.COLUMN);
            }
            if (columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The parameter [column] is required in table mode.");
            }
            if (columns.contains("*")) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The wildcard '*' cannot be used as a writer column.");
            }
            expectedColumns = columns.size();
            if (!IotdbWriterUtil.stringList(conf, IotdbKey.COLUMN_TYPE).isEmpty()) {
                LOG.warn("columnType is only used by the tree model; it is ignored in table mode.");
            }
        }

        int timeColumnPosition = conf.getInt(IotdbKey.TIME_COLUMN_POSITION, 0);
        if (timeColumnPosition < 0 || timeColumnPosition >= expectedColumns) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The parameter [" + IotdbKey.TIME_COLUMN_POSITION + "] must be between 0 and "
                            + (expectedColumns - 1) + ", but was: " + timeColumnPosition);
        }

        int batchSize = conf.getInt(Key.BATCH_SIZE, 1000);
        if (batchSize <= 0) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The parameter [batchSize] must be greater than zero.");
        }

        List<String> preSqls = IotdbWriterUtil.nonBlank(IotdbWriterUtil.stringList(conf, Key.PRE_SQL));
        List<String> postSqls = IotdbWriterUtil.nonBlank(IotdbWriterUtil.stringList(conf, Key.POST_SQL));

        return new IotdbWriterConfig(jdbcUrl, endpoint.host(), endpoint.port(), endpoint.database(), username,
                password, dialect, tree, device, table, List.copyOf(measurements), List.copyOf(columns),
                List.copyOf(columnTypes), timeColumnPosition, batchSize, preSqls, postSqls);
    }

    private static List<TSDataType> parseTypes(List<String> names)
    {
        List<TSDataType> result = new ArrayList<>(names.size());
        for (String name : names) {
            String normalized = name.trim().toUpperCase(Locale.ROOT);
            TSDataType type = switch (normalized) {
                case "BOOLEAN" -> TSDataType.BOOLEAN;
                case "INT32" -> TSDataType.INT32;
                case "INT64" -> TSDataType.INT64;
                case "FLOAT" -> TSDataType.FLOAT;
                case "DOUBLE" -> TSDataType.DOUBLE;
                case "TEXT", "STRING" -> TSDataType.TEXT;
                case "TIMESTAMP" -> TSDataType.TIMESTAMP;
                case "BLOB" -> TSDataType.BLOB;
                default -> throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "Unsupported IoTDB data type in columnType: " + name);
            };
            result.add(type);
        }
        return result;
    }

    private static Endpoint parseEndpoint(String jdbcUrl)
    {
        try {
            // URI understands the authority part: iotdb://host:port/database?options
            URI uri = URI.create(jdbcUrl.substring("jdbc:".length()));
            String host = uri.getHost();
            if (host == null) {
                throw new IllegalArgumentException("no host in " + jdbcUrl);
            }
            int port = uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT;
            String path = uri.getPath();
            String database = StringUtils.trimToNull(path);
            if (database != null) {
                database = database.replaceFirst("^/+", "");
                database = StringUtils.trimToNull(database);
            }
            return new Endpoint(host, port, database);
        }
        catch (RuntimeException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "Invalid IoTDB jdbcUrl, expected jdbc:iotdb://host:port: " + jdbcUrl, e);
        }
    }

    String jdbcUrl()
    {
        return jdbcUrl;
    }

    String host()
    {
        return host;
    }

    int port()
    {
        return port;
    }

    String database()
    {
        return database;
    }

    String username()
    {
        return username;
    }

    String password()
    {
        return password;
    }

    String dialect()
    {
        return dialect;
    }

    boolean tree()
    {
        return tree;
    }

    String device()
    {
        return device;
    }

    String table()
    {
        return table;
    }

    String target()
    {
        return tree ? device : table;
    }

    List<String> measurements()
    {
        return measurements;
    }

    List<String> columns()
    {
        return columns;
    }

    List<TSDataType> columnTypes()
    {
        return columnTypes;
    }

    int timeColumnPosition()
    {
        return timeColumnPosition;
    }

    int batchSize()
    {
        return batchSize;
    }

    List<String> preSqls()
    {
        return preSqls;
    }

    List<String> postSqls()
    {
        return postSqls;
    }

    private record Endpoint(String host, int port, String database)
    {
    }
}
