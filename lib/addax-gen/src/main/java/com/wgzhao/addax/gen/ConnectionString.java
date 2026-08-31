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

package com.wgzhao.addax.gen;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.rdbms.util.DataBaseType;

import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/**
 * Parses the user-friendly connection string used by the addax gen command:
 * <pre>
 *   scheme://user:password@host:port/database
 * </pre>
 * and converts it into a JDBC URL via the scheme table below.
 * <p>
 * The scheme is the standard JDBC URL name (mysql, postgresql, oracle, ...),
 * mapped to the matching {@link DataBaseType} and the dedicated reader/writer
 * plugin pair. Schemes without a dedicated plugin (db2, trino, hive, ...) are
 * deliberately unsupported in v1 because they go through the generic
 * rdbmsreader/rdbmswriter with a jdbcType parameter.
 */
public class ConnectionString
{
    /** Scheme metadata: default port, JDBC URL template and plugin names. */
    private static final class Scheme
    {
        final DataBaseType dataBaseType;
        final int defaultPort;
        final String urlTemplate;
        final String readerPlugin;
        final String writerPlugin;

        Scheme(DataBaseType dataBaseType, int defaultPort, String urlTemplate, String readerPlugin, String writerPlugin)
        {
            this.dataBaseType = dataBaseType;
            this.defaultPort = defaultPort;
            this.urlTemplate = urlTemplate;
            this.readerPlugin = readerPlugin;
            this.writerPlugin = writerPlugin;
        }
    }

    private static final Map<String, Scheme> SCHEMES = Map.ofEntries(
            Map.entry("mysql", new Scheme(DataBaseType.MySql, 3306,
                    "jdbc:mysql://{host}:{port}/{db}?useUnicode=true&characterEncoding=utf-8",
                    "mysqlreader", "mysqlwriter")),
            Map.entry("postgresql", new Scheme(DataBaseType.PostgreSQL, 5432,
                    "jdbc:postgresql://{host}:{port}/{db}",
                    "postgresqlreader", "postgresqlwriter")),
            Map.entry("oracle", new Scheme(DataBaseType.Oracle, 1521,
                    "jdbc:oracle:thin:@//{host}:{port}/{db}",
                    "oraclereader", "oraclewriter")),
            Map.entry("clickhouse", new Scheme(DataBaseType.ClickHouse, 8123,
                    "jdbc:clickhouse://{host}:{port}/{db}",
                    "clickhousereader", "clickhousewriter")),
            Map.entry("sqlserver", new Scheme(DataBaseType.SQLServer, 1433,
                    "jdbc:sqlserver://{host}:{port};DatabaseName={db}",
                    "sqlserverreader", "sqlserverwriter")),
            Map.entry("sybase", new Scheme(DataBaseType.Sybase, 5000,
                    "jdbc:sybase:Tds:{host}:{port}/{db}",
                    "sybasereader", "sybasewriter")),
            Map.entry("hana", new Scheme(DataBaseType.HANA, 30015,
                    "jdbc:sap://{host}:{port}/?databaseName={db}",
                    "hanareader", "hanawriter")),
            Map.entry("sqlite", new Scheme(DataBaseType.SQLite, 0,
                    "jdbc:sqlite:{db}",
                    "sqlitereader", "sqlitewriter")),
            Map.entry("tdengine", new Scheme(DataBaseType.TDengine, 6030,
                    "jdbc:TAOS-RS://{host}:{port}/{db}",
                    "tdenginereader", "tdenginewriter")),
            Map.entry("databend", new Scheme(DataBaseType.Databend, 8000,
                    "jdbc:databend://{host}:{port}/{db}",
                    "databendreader", "databendwriter"))
    );

    private final Scheme scheme;
    private final String username;
    private final String password;
    private final String host;
    private final int port;
    private final String database;

    private ConnectionString(Scheme scheme, String username, String password,
            String host, int port, String database)
    {
        this.scheme = scheme;
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.database = database;
    }

    /**
     * Parses a connection string. Passwords containing '@' or ':' are not
     * supported inline — use the --password-env option instead.
     *
     * @param input the raw connection string, e.g. mysql://user:pass@host:3306/db
     * @return the parsed connection string
     */
    public static ConnectionString parse(String input)
    {
        int schemeEnd = input.indexOf("://");
        if (schemeEnd <= 0) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "Invalid connection string (expected scheme://user:pass@host:port/db): " + input);
        }
        String schemeName = input.substring(0, schemeEnd).toLowerCase();
        Scheme scheme = SCHEMES.get(schemeName);
        if (scheme == null) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "Unsupported scheme '" + schemeName + "' — supported schemes: " + String.join(", ", SCHEMES.keySet()));
        }

        String rest = input.substring(schemeEnd + 3);
        String username = null;
        String password = null;
        String hostPart = rest;
        int atIndex = rest.lastIndexOf('@');
        if (atIndex >= 0) {
            String userInfo = rest.substring(0, atIndex);
            hostPart = rest.substring(atIndex + 1);
            int colon = userInfo.indexOf(':');
            if (colon >= 0) {
                username = userInfo.substring(0, colon);
                password = userInfo.substring(colon + 1);
            }
            else {
                username = userInfo;
            }
        }

        String host;
        int port;
        String database;
        if (schemeName.equals("sqlite")) {
            // sqlite has no host/port; the path is the database file
            host = "localhost";
            port = 0;
            database = hostPart;
        }
        else {
            int slash = hostPart.indexOf('/');
            String hostPort = slash >= 0 ? hostPart.substring(0, slash) : hostPart;
            database = slash >= 0 ? hostPart.substring(slash + 1) : "";
            if (database.isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Missing database name in connection string: " + input);
            }
            int colon = hostPort.indexOf(':');
            if (colon >= 0) {
                host = hostPort.substring(0, colon);
                port = Integer.parseInt(hostPort.substring(colon + 1));
            }
            else {
                host = hostPort;
                port = scheme.defaultPort;
            }
            if (host.isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Missing host in connection string: " + input);
            }
        }
        return new ConnectionString(scheme, username, password, host, port, database);
    }

    public DataBaseType dataBaseType()
    {
        return scheme.dataBaseType;
    }

    public String username()
    {
        return username;
    }

    public String password()
    {
        return password;
    }

    public String database()
    {
        return database;
    }

    public String readerPlugin()
    {
        return scheme.readerPlugin;
    }

    public String writerPlugin()
    {
        return scheme.writerPlugin;
    }

    public String toJdbcUrl()
    {
        return scheme.urlTemplate
                .replace("{host}", host)
                .replace("{port}", String.valueOf(port))
                .replace("{db}", database);
    }

    @Override
    public String toString()
    {
        return schemeName() + "://" + host + ":" + port + "/" + database;
    }

    private String schemeName()
    {
        return scheme.dataBaseType.getTypeName();
    }
}
