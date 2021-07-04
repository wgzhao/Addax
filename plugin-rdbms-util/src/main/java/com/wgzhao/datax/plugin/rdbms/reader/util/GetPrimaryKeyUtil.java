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
package com.wgzhao.datax.plugin.rdbms.reader.util;

import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.Key;
import com.wgzhao.datax.plugin.rdbms.util.DBUtil;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class GetPrimaryKeyUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(GetPrimaryKeyUtil.class);

    public static DataBaseType dataBaseType;

    public static final String QUERY_PRIMARY_KEY_FOR_TABLE = "SELECT ALL_CONS_COLUMNS.COLUMN_NAME FROM ALL_CONS_COLUMNS, "
            + "ALL_CONSTRAINTS WHERE ALL_CONS_COLUMNS.CONSTRAINT_NAME = "
            + "ALL_CONSTRAINTS.CONSTRAINT_NAME AND "
            + "ALL_CONSTRAINTS.CONSTRAINT_TYPE = 'P' AND "
            + "ALL_CONS_COLUMNS.TABLE_NAME = ? AND "
            + "ALL_CONS_COLUMNS.OWNER = ?";

    public static final String QUERY_GET_SESSIONUSER = "SELECT USER FROM DUAL";

    private GetPrimaryKeyUtil()
    {
    }

    /**
     * 尝试自动获取指定表的主键，如果有多个，则取第一个
     *
     * @param readConf 读配置项
     * @return 主键
     */
    public static String getPrimaryKey(Configuration readConf)
    {
        String pk = null;
        String sql;
        Configuration connConf = Configuration.from(readConf.getList("connection").get(0).toString());
        String table = connConf.getList("table").get(0).toString();
        String jdbc_url = connConf.getString(Key.JDBC_URL);
        String username = readConf.getString(Key.USERNAME, null);
        String password = readConf.getString(Key.PASSWORD, null);
        String schema = null;
        if (table.contains(".")) {
            schema = table.split("\\.")[1];
            table = table.split("\\.")[0];
        }

        try (Connection connection = DBUtil.getConnection(dataBaseType, jdbc_url, username, password)) {
            if (dataBaseType == DataBaseType.Oracle) {
                pk = getOraclePrimaryKey(connection, schema, table);
            }
            else {
                sql = getPrimaryKeyQuery(schema, table);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                // TODO select the appropriate column instead of the first column based data type, numeric type is prefered
                if (resultSet.next()) {
                    pk = resultSet.getString(1);
                }
            }
        }
        catch (SQLException ignore) {
        }
        return pk;
    }

    /**
     * 依据不同数据库类型，返回对应的获取主键的SQL语句
     *
     * @param schema schema
     * @param tableName 要查询的表
     * @return 获取主键 SQL 语句
     */
    public static String getPrimaryKeyQuery(String schema, String tableName)
    {
        String sql = null;
        switch (dataBaseType) {
            case MySql:
                sql = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_SCHEMA = ( " + getSchema(schema) + ")"
                        + "AND TABLE_NAME = '" + tableName + "' "
                        + "AND COLUMN_KEY = 'PRI'";
                break;
            case PostgreSQL:
                sql = "SELECT col.ATTNAME FROM PG_CATALOG.PG_NAMESPACE sch, "
                        + "  PG_CATALOG.PG_CLASS tab, PG_CATALOG.PG_ATTRIBUTE col, "
                        + "  PG_CATALOG.PG_INDEX ind "
                        + "WHERE sch.OID = tab.RELNAMESPACE "
                        + "  AND tab.OID = col.ATTRELID "
                        + "  AND tab.OID = ind.INDRELID "
                        + "  AND sch.NSPNAME = (" + getSchema(schema) + ") "
                        + "  AND tab.RELNAME = '" + tableName + "' "
                        + "  AND col.ATTNUM = ANY(ind.INDKEY) "
                        + "  AND ind.INDISPRIMARY";
                break;
            case SQLServer:
                sql = "SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc, "
                        + "  INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
                        + "WHERE tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
                        + "  AND tc.TABLE_NAME = kcu.TABLE_NAME "
                        + "  AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
                        + "  AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                        + "  AND tc.TABLE_SCHEMA = (" + getSchema(schema) + ") "
                        + "  AND tc.TABLE_NAME = N'" + tableName + "' "
                        + "  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'";
                break;
            case ClickHouse:
                sql = "SELECT  name FROM system.columns "
                        + " WHERE database = (" + getSchema(schema) + ") "
                        + " AND table = '" + tableName + "'"
                        + "AND is_in_primary_key = 1";
            default:
                break;
        }
        return sql;
    }

    /**
     * get current schema
     *
     * @param schema schema name
     * @return schema name or get schema expression
     */
    public static String getSchema(String schema)
    {
        if (schema != null) {
            return "'" + schema + "'";
        }
        switch (dataBaseType) {
            case MySql:
                schema = "SELECT SCHEMA()";
                break;
            case PostgreSQL:
                schema = "SELECT CURRENT_SCHEMA()";
                break;
            case SQLServer:
                schema = "SELECT SCHEMA_NAME()";
                break;
            case ClickHouse:
                schema = "SELECT currentDatabase()";
                break;
            default:
                break;
        }
        return schema;
    }


    public static String getOraclePrimaryKey(Connection conn, String tableOwner, String tableName)
    {
        PreparedStatement pStmt = null;
        ResultSet rset = null;
        List<String> columns = new ArrayList<>();

        try {
            if (tableOwner == null) {
                tableOwner = getSessionUser(conn);
            }

            pStmt = conn.prepareStatement(QUERY_PRIMARY_KEY_FOR_TABLE,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            LOG.debug("Get Oracle primary key's SQL: [{}], params [{},{}]", QUERY_PRIMARY_KEY_FOR_TABLE, tableOwner, tableName);
            pStmt.setString(1, tableName);
            pStmt.setString(2, tableOwner);
            rset = pStmt.executeQuery();

            while (rset.next()) {
                columns.add(rset.getString(1));
            }
        }
        catch (SQLException e) {
            LOG.debug("Failed to guess primary key: {}", e.getMessage());
        }
        finally {
            if (rset != null) {
                try {
                    rset.close();
                }
                catch (SQLException ignored) {
                }
            }
            if (pStmt != null) {
                try {
                    pStmt.close();
                }
                catch (SQLException ignored) {
                }
            }
        }

        if (columns.size() == 0) {
            // Table has no primary key
            LOG.debug("table {} has no primary key", tableName);
            return null;
        }

        if (columns.size() > 1) {
            // The primary key is multi-column primary key. Warn the user.
            // TODO select the appropriate column instead of the first column based
            // on the datatype - giving preference to numerics over other types.
            LOG.warn("The table " + tableName + " "
                    + "contains a multi-column primary key. Sqoop will default to "
                    + "the column " + columns.get(0) + " only for this job.");
        }

        return columns.get(0);
    }

    public static String getSessionUser(Connection conn)
    {
        Statement stmt = null;
        ResultSet rset = null;
        String user = null;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rset = stmt.executeQuery(QUERY_GET_SESSIONUSER);

            if (rset.next()) {
                user = rset.getString(1);
            }
            conn.commit();
        }
        catch (SQLException e) {
            try {
                conn.rollback();
            }
            catch (SQLException ignored) {
            }
        }
        finally {
            if (rset != null) {
                try {
                    rset.close();
                }
                catch (SQLException ignored) {
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException ignored) {
                }
            }
        }
        if (user == null) {
            throw new RuntimeException("Unable to get current session user");
        }
        return user;
    }
}
