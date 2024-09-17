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
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.base.Key.CONNECTION;

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
        Configuration connConf = readConf.getConfiguration(CONNECTION);
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
            List<String[]> columns = new ArrayList<>();
            while (resultSet.next()) {
                // column_name, data_type
                columns.add(new String[] {resultSet.getString(1), resultSet.getString(2)});
            }

            if (columns.isEmpty()) {
                // Table has no primary key
                LOG.debug("The table {} has no primary key", table);
                return null;
            }

            String selectColumn = columns.get(0)[0];

            if (columns.size() > 1) {
                // The primary key is multi-column primary key.
                // select the appropriate column instead of the first column based
                // on the datatype - giving preference to numerics over other types.
                LOG.warn("The table {} contains a multi-column primary key. try to select numeric type primary key if present", table);
                selectColumn = columns.get(0)[0];
                JDBCType jdbcType;
                for (String[] column : columns) {
                    try {
                        jdbcType = JDBCType.valueOf(column[1]);
                        if (jdbcType == JDBCType.NUMERIC || jdbcType == JDBCType.INTEGER
                                || jdbcType == JDBCType.BIGINT || jdbcType == JDBCType.DECIMAL
                                || jdbcType == JDBCType.FLOAT) {
                            // better choice
                            selectColumn = column[0];
                            break;
                        }
                    }
                    catch (IllegalArgumentException ignored) {
                        // ignore
                    }
                }
                return selectColumn;
            }
            return selectColumn;
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
                sql = "select c.COLUMN_NAME, c.DATA_TYPE " +
                        "from INFORMATION_SCHEMA.`COLUMNS` c , INFORMATION_SCHEMA.STATISTICS s " +
                        "where c.TABLE_SCHEMA = s.TABLE_SCHEMA " +
                        " AND c.TABLE_NAME = s.TABLE_NAME " +
                        " AND c.COLUMN_NAME = s.COLUMN_NAME " +
                        " AND s.TABLE_SCHEMA = (" + getSchema(schema) + ") " +
                        " AND s.TABLE_NAME = '" + tableName + "' " +
                        " AND NON_UNIQUE = 0 " +
                        "ORDER BY SEQ_IN_INDEX ASC";
                break;
            case PostgreSQL:
                sql = "SELECT col.ATTNAME, PG_CATALOG.FORMAT_TYPE(col.ATTTYPID, col.ATTTYPMOD) AS DTYPE "
                        + "  FROM PG_CATALOG.PG_NAMESPACE sch, "
                        + "  PG_CATALOG.PG_CLASS tab, PG_CATALOG.PG_ATTRIBUTE col, "
                        + "  PG_CATALOG.PG_INDEX ind "
                        + "  WHERE sch.OID = tab.RELNAMESPACE "
                        + "  AND tab.OID = col.ATTRELID "
                        + "  AND tab.OID = ind.INDRELID "
                        + "  AND col.ATTNUM > 0 "
                        + "  AND sch.NSPNAME = (" + getSchema(schema) + ") "
                        + "  AND tab.RELNAME = '" + tableName + "' "
                        + "  AND col.ATTNUM = ANY(ind.INDKEY) "
                        + "  AND (ind.INDISPRIMARY OR ind.INDISUNIQUE)";
                break;
            case SQLServer:
                sql = "SELECT  COL_NAME(ic.OBJECT_ID, ic.column_id) AS ColumnName, t.name AS DataType " +
                        "FROM " +
                        "    sys.indexes AS i " +
                        "    INNER JOIN sys.index_columns AS ic ON i.OBJECT_ID = ic.OBJECT_ID " +
                        "    AND i.index_id = ic.index_id " +
                        "    INNER JOIN sys.columns AS c ON ic.OBJECT_ID = c.OBJECT_ID " +
                        "    AND ic.column_id = c.column_id " +
                        "    INNER JOIN sys.types AS t ON c.system_type_id = t.system_type_id " +
                        "    AND c.user_type_id = t.user_type_id " +
                        "WHERE " +
                        "    OBJECT_NAME(ic.OBJECT_ID) = '" + tableName +  "' " +
                        "    AND i.is_unique = 1 " +
                        "    AND (SELECT COUNT(*) FROM sys.index_columns " +
                        "        WHERE OBJECT_ID = i.OBJECT_ID  AND index_id = i.index_id ) = 1";
                break;
            case ClickHouse:
                sql = "SELECT name, type FROM system.columns "
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
                sql = "SELECT AC.COLUMN_NAME, AC.DATA_TYPE  "
                        + "FROM ALL_INDEXES AI,  ALL_IND_COLUMNS AIC, ALL_TAB_COLUMNS AC "
                        + "WHERE  AI.TABLE_NAME = AC.TABLE_NAME "
                        + "AND AI.OWNER = AC.OWNER "
                        + "AND AI.TABLE_NAME  = AIC.TABLE_NAME "
                        + "AND AI.INDEX_NAME = AIC.INDEX_NAME"
                        + "AND AC.COLUMN_NAME  = AIC.COLUMN_NAME"
                        + "AND AI.OWNER = '" + schema + "' "
                        + "AND AI.UNIQUENESS = 'UNIQUE' "
                        + "AND AI.TABLE_NAME = '" + tableName + "'";
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
