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

package com.wgzhao.addax.rdbms.util;

import com.wgzhao.addax.core.exception.AddaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;

/**
 * Schema introspection helpers used by the addax gen command and other tools
 * that need to discover tables, primary keys and column metadata at runtime.
 * <p>
 * All methods take a database name (catalog or schema) that is tried as the
 * JDBC catalog first and as the schema second, because drivers disagree on
 * where the database name lives in the metadata model.
 */
public class SchemaProbe
{
    private static final Logger LOG = LoggerFactory.getLogger(SchemaProbe.class);

    /**
     * Column metadata gathered from JDBC ResultSetMetaData, plus primary key info.
     */
    public static class ColumnInfo
    {
        public final String name;
        public final String typeName;
        public final int precision;
        public final int scale;
        public final boolean primaryKey;

        ColumnInfo(String name, String typeName, int precision, int scale, boolean primaryKey)
        {
            this.name = name;
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.primaryKey = primaryKey;
        }
    }

    private SchemaProbe() {}

    /**
     * Loads the JDBC driver for the given database type so the connection
     * can be established. The driver jar is expected on the runtime classpath.
     */

    private static Connection connect(DataBaseType dataBaseType, String jdbcUrl, String username, String password)
            throws SQLException
    {
        return java.sql.DriverManager.getConnection(jdbcUrl, username, password);
    }

    public static void loadDriver(DataBaseType dataBaseType)
    {
        try {
            Class.forName(dataBaseType.getDriverClassName());
        }
        catch (ClassNotFoundException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Driver class not found: " + dataBaseType.getDriverClassName()
                            + " — the driver jar must be present on the classpath", e);
        }
    }

    /**
     * Lists user tables in the given database. The database name is tried as
     * the JDBC catalog, then as the schema, then in both positions, then as a
     * last resort all tables across schemas (system schemas excluded), because
     * drivers disagree on where the database name lives in the metadata model.
     */
    public static List<String> listTables(DataBaseType dataBaseType, String jdbcUrl,
            String username, String password, String database)
    {
        loadDriver(dataBaseType);
        // plain DriverManager connection instead of DBUtil's DBCP pool, because DBCP
        // scans every Driver on the classpath and fails on unrelated plugins' drivers
        try (Connection conn = connect(dataBaseType, jdbcUrl, username, password)) {
            List<String> tables = listTables(conn, database, database);
            if (tables.isEmpty()) {
                tables = listTables(conn, null, database);
            }
            if (tables.isEmpty()) {
                tables = listTables(conn, database, null);
            }
            if (tables.isEmpty()) {
                tables = listTables(conn, null, null);
            }
            return tables;
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to list tables", e);
        }
    }

    /**
     * Returns the primary key columns of a table, ordered by KEY_SEQ.
     */
    public static List<String> getPrimaryKeys(DataBaseType dataBaseType, String jdbcUrl,
            String username, String password, String database, String table)
    {
        loadDriver(dataBaseType);
        // plain DriverManager connection instead of DBUtil's DBCP pool, because DBCP
        // scans every Driver on the classpath and fails on unrelated plugins' drivers
        try (Connection conn = connect(dataBaseType, jdbcUrl, username, password)) {
            return getPrimaryKeys(conn, database, table);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to read primary key of table " + table, e);
        }
    }

    /**
     * Returns column metadata for a table, including primary key flags.
     */
    public static List<ColumnInfo> getColumns(DataBaseType dataBaseType, String jdbcUrl,
            String username, String password, String database, String table)
    {
        loadDriver(dataBaseType);
        // plain DriverManager connection instead of DBUtil's DBCP pool, because DBCP
        // scans every Driver on the classpath and fails on unrelated plugins' drivers
        try (Connection conn = connect(dataBaseType, jdbcUrl, username, password)) {
            return getColumns(conn, database, table);
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to read columns of table " + table, e);
        }
    }

    /**
     * Builds a map of column name to JDBC type name for quick name matching.
     */
    public static Map<String, String> columnTypes(List<ColumnInfo> columns)
    {
        Map<String, String> result = new HashMap<>();
        for (ColumnInfo column : columns) {
            result.put(column.name.toLowerCase(), column.typeName);
        }
        return result;
    }

    private static List<String> listTables(Connection conn, String catalog, String schema)
            throws SQLException
    {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getTables(catalog, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String schemaName = rs.getString("TABLE_SCHEM");
                // skip system schemas when falling back to a full listing
                if (schema == null && schemaName != null
                        && (schemaName.startsWith("pg_") || "information_schema".equals(schemaName))) {
                    continue;
                }
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    private static List<String> getPrimaryKeys(Connection conn, String catalog, String table)
            throws SQLException
    {
        // drivers disagree on where the database name lives (catalog vs schema),
        // so fall back to no catalog when the first attempt returns nothing
        List<String> keys = readPrimaryKeys(conn, catalog, table);
        if (keys.isEmpty()) {
            keys = readPrimaryKeys(conn, null, table);
        }
        return keys;
    }

    private static List<String> readPrimaryKeys(Connection conn, String catalog, String table)
            throws SQLException
    {
        DatabaseMetaData metaData = conn.getMetaData();
        List<String[]> keys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(catalog, null, table)) {
            while (rs.next()) {
                keys.add(new String[] {rs.getString("KEY_SEQ"), rs.getString("COLUMN_NAME")});
            }
        }
        keys.sort((a, b) -> Integer.compare(Integer.parseInt(a[0]), Integer.parseInt(b[0])));
        List<String> result = new ArrayList<>();
        for (String[] key : keys) {
            result.add(key[1]);
        }
        return result;
    }

    private static List<ColumnInfo> getColumns(Connection conn, String catalog, String table)
            throws SQLException
    {
        Set<String> pkColumns = new LinkedHashSet<>(getPrimaryKeys(conn, catalog, table));
        List<Map<String, Object>> meta = DBUtil.getColumnMetaData(conn, table, "*");
        List<ColumnInfo> columns = new ArrayList<>();
        // index 0 is a null placeholder kept by getColumnMetaData
        for (int i = 1; i < meta.size(); i++) {
            Map<String, Object> row = meta.get(i);
            columns.add(new ColumnInfo(
                    (String) row.get("name"),
                    (String) row.get("typeName"),
                    (Integer) row.get("precision"),
                    (Integer) row.get("scale"),
                    pkColumns.contains(((String) row.get("name")).toLowerCase())));
        }
        return columns;
    }
}
