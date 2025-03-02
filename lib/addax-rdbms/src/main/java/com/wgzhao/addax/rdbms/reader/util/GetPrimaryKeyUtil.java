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
import java.util.Arrays;
import java.util.List;

import static com.wgzhao.addax.common.base.Key.CONNECTION;
import static com.wgzhao.addax.common.base.Key.JDBC_URL;
import static com.wgzhao.addax.common.base.Key.PASSWORD;
import static com.wgzhao.addax.common.base.Key.TABLE;
import static com.wgzhao.addax.common.base.Key.USERNAME;

public class GetPrimaryKeyUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(GetPrimaryKeyUtil.class);

    public static DataBaseType dataBaseType;

    private GetPrimaryKeyUtil()
    {
    }

    /**
     * Try to get a primary key or unique key on single column to split the data
     * if no primary key or unique key, return null
     * Give priority to selecting the primary key, followed by a unique index of numeric type,
     * and lastly, other divisible unique indexes.
     *
     * @param readConf {@link Configuration}
     * @return column name if it has primary key or unique key, else null
     */
    public static String getPrimaryKey(Configuration readConf)
    {
        String sql;
        List<String[]> columns = new ArrayList<>();
        Configuration connConf = readConf.getConfiguration(CONNECTION);
        String table = connConf.getList(TABLE).get(0).toString();
        String jdbc_url = connConf.getString(JDBC_URL);
        String username = readConf.getString(USERNAME, null);
        String password = readConf.getString(PASSWORD, null);
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

            while (resultSet.next()) {
                // column_name, data_type
                columns.add(new String[] {resultSet.getString(1), resultSet.getString(2)});
            }
        }
        catch (SQLException e) {
            LOG.debug(e.getMessage());
            return null;
        }

        if (columns.isEmpty()) {
            // Table has no primary key
            LOG.debug("The table {} has no primary key", table);
            return null;
        }

        if (columns.size() == 1) {
            return columns.get(0)[0];
        }

        LOG.warn("The table {} contains a multiply candidate keys. try to choose numeric type key if present", table);
        JDBCType jdbcType;
        List<JDBCType> numericTypes = Arrays.asList(
                JDBCType.NUMERIC,
                JDBCType.INTEGER,
                JDBCType.BIGINT,
                JDBCType.DECIMAL,
                JDBCType.FLOAT
        );
        for (String[] column : columns) {
            // JDBCType not support INT type, it exists in MySQL
            if ("INT".equals(column[1])) {
                // better choice
                return column[0];
            }
            try {
                jdbcType = JDBCType.valueOf(column[1]);
            }
            catch (IllegalArgumentException e) {
                LOG.warn("The column type {} does not map to JDBCType", column[1]);
                continue;
            }
            if (numericTypes.contains(jdbcType)) {
                // better choice
                return column[0];
            }
        }
        // last choice
        return columns.get(0)[0];
    }

    /**
     * generate SQL to get primary key
     *
     * @param schema schema
     * @param tableName the table name
     * @param username username
     * @return the sql string to get primary key
     */
    public static String getPrimaryKeyQuery(String schema, String tableName, String username)
    {
        String sql = null;
        switch (dataBaseType) {
            case MySql:
                sql = "select "
                        + " c.COLUMN_NAME, upper(c.DATA_TYPE) AS COLUMN_TYPE, c.COLUMN_KEY AS KEY_TYPE "
                        + " from INFORMATION_SCHEMA.`COLUMNS` c , INFORMATION_SCHEMA.STATISTICS s "
                        + " where c.TABLE_SCHEMA = s.TABLE_SCHEMA "
                        + "  AND c.TABLE_NAME = s.TABLE_NAME "
                        + "  AND c.COLUMN_NAME = s.COLUMN_NAME "
                        + "  AND s.TABLE_SCHEMA = (SELECT SCHEMA()) "
                        + "  AND s.TABLE_NAME = '" + tableName + "' "
                        + "  AND NON_UNIQUE = 0 "
                        + " AND COLUMN_KEY <> 'MUL' and COLUMN_KEY <> '' "
                        + " ORDER BY c.COLUMN_KEY ASC, c.DATA_TYPE ASC";
                break;
            case PostgreSQL:
                sql = "SELECT a.attname AS COLUMN_NAME, "
                        + " upper(format_type(a.atttypid, a.atttypmod)) AS COLUMN_TYPE, "
                        + " CASE WHEN con.contype = 'p' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE "
                        + " FROM pg_constraint con "
                        + " JOIN pg_class rel ON rel.oid = con.conrelid "
                        + " JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace "
                        + " LEFT JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = con.conrelid "
                        + " WHERE nsp.nspname = (SELECT CURRENT_SCHEMA()) "
                        + " AND rel.relname = '" + tableName + "'"
                        + " AND con.contype IN ('p', 'u') AND array_length(con.conkey, 1) = 1"
                        + " ORDER BY con.contype ASC, a.atttypid ASC";
                break;
            case SQLServer:
                sql = "SELECT "
                        + "    kc.COLUMN_NAME, "
                        + "    upper(c.DATA_TYPE) AS COLUMN_TYPE, "
                        + "    CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE "
                        + " FROM "
                        + "    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                        + "    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME "
                        + "    JOIN INFORMATION_SCHEMA.COLUMNS c ON kc.TABLE_NAME = c.TABLE_NAME AND kc.COLUMN_NAME = c.COLUMN_NAME "
                        + " WHERE "
                        + "    tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')  "
                        + "    AND kc.TABLE_SCHEMA = (select schema_name())  "
                        + "    AND kc.TABLE_NAME = '" + tableName + "'  "
                        + "    AND (SELECT COUNT(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = kc.CONSTRAINT_NAME) = 1"
                        + " ORDER BY tc.CONSTRAINT_TYPE ASC, c.DATA_TYPE ASC";
                break;
            case ClickHouse:
                sql = "SELECT name as column_name, type as column_type, 'PRI' as key_type"
                        + " FROM system.columns "
                        + " WHERE database = (SELECT currentDatabase()) "
                        + " AND table = '" + tableName + "'"
                        + " AND is_in_primary_key = 1"
                        + " ORDER BY type ASC";
                break;
            case Oracle:
                schema = schema == null ? username.toUpperCase() : schema.toUpperCase();
                // 表明如果没有强制原始大小写，则一律转为大写
                if (!tableName.startsWith("\"")) {
                    tableName = tableName.toUpperCase();
                }
                sql = "SELECT acc.column_name, upper(cc.data_type) AS COLUMN_TYPE, "
                        + " CASE WHEN ac.constraint_type = 'P' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE "
                        + "FROM "
                        + "    all_constraints ac "
                        + "    JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name "
                        + "    JOIN all_tab_columns cc ON acc.table_name = cc.table_name AND acc.column_name = cc.column_name "
                        + "WHERE "
                        + "    ac.constraint_type IN ('P', 'U')  "
                        + "    AND ac.owner = '" + schema + "' "
                        + "    AND acc.table_name = '" + tableName + "' "
                        + "    AND (SELECT COUNT(*) FROM all_cons_columns WHERE constraint_name = ac.constraint_name) = 1"
                        + " ORDER BY ac.constraint_type ASC, cc.data_type ASC";
                break;
            default:
                break;
        }
        return sql;
    }
}
