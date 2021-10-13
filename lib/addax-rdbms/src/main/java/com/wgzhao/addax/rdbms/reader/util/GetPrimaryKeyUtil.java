/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package com.wgzhao.addax.rdbms.reader.util;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class GetPrimaryKeyUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(GetPrimaryKeyUtil.class);

    public static DataBaseType dataBaseType;

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
        String sql;
        Configuration connConf = Configuration.from(readConf.getList("connection").get(0).toString());
        String table = connConf.getList("table").get(0).toString();
        String jdbc_url = connConf.getString(Key.JDBC_URL);
        String username = readConf.getString(Key.USERNAME, null);
        String password = readConf.getString(Key.PASSWORD, null);
        String schema = null;
        if (table.contains(".")) {
            schema = table.split("\\.")[0];
            table = table.split("\\.")[1];
        }

        try (Connection connection = DBUtil.getConnection(dataBaseType, jdbc_url, username, password)) {
            sql = getPrimaryKeyQuery(schema, table, username);
            if (sql == null) {
                LOG.debug("The current database is unsupported yet.");
                return null;
            }
            LOG.debug("query primary sql: [{}]", sql);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> columns = new ArrayList<>();
            while (resultSet.next()) {
                columns.add(resultSet.getString(1));
            }
            if (columns.isEmpty()) {
                // Table has no primary key
                LOG.debug("The table {} has no primary key", table);
                return null;
            }

            if (columns.size() > 1) {
                // The primary key is multi-column primary key. Warn the user.
                // TODO select the appropriate column instead of the first column based
                // on the datatype - giving preference to numerics over other types.
                LOG.warn("The table " + table + " "
                        + "contains a multi-column primary key. Addax will take"
                        + "the column " + columns.get(0) + " as primary key for this job by default");
            }
            return columns.get(0);
        }
        catch (SQLException e) {
            LOG.debug(e.getMessage());
        }
        return null;
    }

    /**
     * 依据不同数据库类型，返回对应的获取主键的SQL语句
     *
     * @param schema schema
     * @param tableName 要查询的表
     * @param username username
     * @return 获取主键 SQL 语句
     */
    public static String getPrimaryKeyQuery(String schema, String tableName, String username)
    {
        String sql = null;
        switch (dataBaseType) {
            case MySql:
                /*
                only query primary key

                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = (" + getSchema(schema) + ") "
                AND TABLE_NAME = '" + tableName + "'"
                AND COLUMN_KEY = 'PRI'
                 */
                sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS "
                        + " WHERE TABLE_SCHEMA = (" + getSchema(schema) + ") "
                        + " AND TABLE_NAME = '" + tableName + "'"
                        + " AND NON_UNIQUE = 0 ORDER BY SEQ_IN_INDEX ASC";
                break;
            case PostgreSQL:
                sql = "SELECT col.ATTNAME FROM PG_CATALOG.PG_NAMESPACE sch, "
                        + "  PG_CATALOG.PG_CLASS tab, PG_CATALOG.PG_ATTRIBUTE col, "
                        + "  PG_CATALOG.PG_INDEX ind "
                        + "  WHERE sch.OID = tab.RELNAMESPACE "
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
                        + "  WHERE tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
                        + "  AND tc.TABLE_NAME = kcu.TABLE_NAME "
                        + "  AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
                        + "  AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                        + "  AND tc.TABLE_SCHEMA = (" + getSchema(schema) + ") "
                        + "  AND tc.TABLE_NAME = '" + tableName + "' "
                        + "  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'";
                break;
            case ClickHouse:
                sql = "SELECT name FROM system.columns "
                        + " WHERE database = (" + getSchema(schema) + ") "
                        + " AND table = '" + tableName + "'"
                        + " AND is_in_primary_key = 1";
                break;
            case Oracle:
                if (schema == null) {
                    schema = username.toUpperCase();
                }
                else {
                    schema = schema.toUpperCase();
                }
                // 表明如果没有强制原始大小写，则一律转为大写
                if (!tableName.startsWith("\"")) {
                    tableName = tableName.toUpperCase();
                }
                sql = "SELECT AC.COLUMN_NAME " +
                        "FROM ALL_INDEXES AI, ALL_IND_COLUMNS AC " +
                        "WHERE  AI.TABLE_NAME = AC.TABLE_NAME " +
                        "AND AI.INDEX_NAME = AC.INDEX_NAME " +
                        "AND AI.OWNER = '" + schema + "' " +
                        "AND AI.UNIQUENESS = 'UNIQUE' " +
                        "AND AI.TABLE_NAME = '" + tableName + "'";
                break;
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
}
