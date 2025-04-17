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

import com.wgzhao.addax.core.util.Configuration;
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

import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.JDBC_URL;
import static com.wgzhao.addax.core.base.Key.PASSWORD;
import static com.wgzhao.addax.core.base.Key.TABLE;
import static com.wgzhao.addax.core.base.Key.USERNAME;

public class GetPrimaryKeyUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(GetPrimaryKeyUtil.class);

    private GetPrimaryKeyUtil()
    {
    }

    /**
     * Try to get a primary key or unique key on single column to split the data
     * if no primary key or unique key, return null
     * Give priority to selecting the primary key, followed by a unique index of numeric type,
     * and lastly, other divisible unique indexes.
     *
     * @param dataBaseType {@link DataBaseType}
     * @param readConf {@link Configuration}
     * @return column name if it has primary key or unique key, else null
     */
    public static String getPrimaryKey(DataBaseType dataBaseType, Configuration readConf)
    {
        String sql;
        List<String[]> columns = new ArrayList<>();
        Configuration connConf = readConf.getConfiguration(CONNECTION);
        String table = connConf.getList(TABLE).get(0).toString();
        String jdbcUrl = connConf.getString(JDBC_URL);
        String username = readConf.getString(USERNAME, null);
        String password = readConf.getString(PASSWORD, null);
        String schema = null;
        if (table.contains(".")) {
            schema = table.split("\\.")[0];
            table = table.split("\\.")[1];
        }

        sql = getPrimaryKeyQuery(dataBaseType, schema, table, username);
        if (sql == null) {
            LOG.debug("The current database is unsupported yet.");
            return null;
        }

        try (Connection connection = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {

            LOG.debug("query primary sql: [{}]", sql);

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
        List<JDBCType> numericTypes = Arrays.asList(JDBCType.NUMERIC, JDBCType.INTEGER, JDBCType.BIGINT, JDBCType.DECIMAL, JDBCType.FLOAT);
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
     * Generate SQL to get primary key or unique single-column key
     *
     * @param schema schema name, can be null
     * @param tableName the table name
     * @param username username (used for Oracle when schema is null)
     * @return the sql string to get primary key
     */
    public static String getPrimaryKeyQuery(DataBaseType dataBaseType, String schema, String tableName, String username)
    {
        if (dataBaseType == null) {
            LOG.warn("Database type is null, cannot generate primary key query");
            return null;
        }

        StringBuilder sql = new StringBuilder();

        switch (dataBaseType) {
            case MySql:
                sql.append("select ")
                        .append(" c.COLUMN_NAME, upper(c.DATA_TYPE) AS COLUMN_TYPE, c.COLUMN_KEY AS KEY_TYPE ")
                        .append(" from INFORMATION_SCHEMA.`COLUMNS` c , INFORMATION_SCHEMA.STATISTICS s ")
                        .append(" where c.TABLE_SCHEMA = s.TABLE_SCHEMA ")
                        .append("  AND c.TABLE_NAME = s.TABLE_NAME ")
                        .append("  AND c.COLUMN_NAME = s.COLUMN_NAME ")
                        .append("  AND s.TABLE_SCHEMA = ")
                        .append(schema == null ? "(SELECT SCHEMA()) " : "'" + schema + "'")
                        .append("  AND s.TABLE_NAME = '").append(tableName).append("' ")
                        .append("  AND NON_UNIQUE = 0 ")
                        .append(" AND COLUMN_KEY <> 'MUL' and COLUMN_KEY <> '' ")
                        .append(" ORDER BY c.COLUMN_KEY ASC, c.DATA_TYPE ASC");
                break;
            case PostgreSQL:
                sql.append("SELECT a.attname AS COLUMN_NAME, ")
                        .append(" upper(format_type(a.atttypid, a.atttypmod)) AS COLUMN_TYPE, ")
                        .append(" CASE WHEN con.contype = 'p' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE ")
                        .append(" FROM pg_constraint con ")
                        .append(" JOIN pg_class rel ON rel.oid = con.conrelid ")
                        .append(" JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace ")
                        .append(" LEFT JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = con.conrelid ")
                        .append(" WHERE nsp.nspname = ")
                        .append(schema == null ? "(SELECT CURRENT_SCHEMA()) " : "'" + schema + "'")
                        .append(" AND rel.relname = '").append(tableName).append("'")
                        .append(" AND con.contype IN ('p', 'u') AND array_length(con.conkey, 1) = 1")
                        .append(" ORDER BY con.contype ASC, a.atttypid ASC");
                break;
            case SQLServer:
                sql.append("SELECT ")
                        .append("    kc.COLUMN_NAME, ")
                        .append("    upper(c.DATA_TYPE) AS COLUMN_TYPE, ")
                        .append("    CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE ")
                        .append(" FROM ")
                        .append("    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ")
                        .append("    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME ")
                        .append("    JOIN INFORMATION_SCHEMA.COLUMNS c ON kc.TABLE_NAME = c.TABLE_NAME AND kc.COLUMN_NAME = c.COLUMN_NAME ")
                        .append(" WHERE ")
                        .append("    tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')  ")
                        .append("    AND kc.TABLE_SCHEMA = ")
                        .append(schema == null ? "(select schema_name())  " : "'" + schema + "'")
                        .append("    AND kc.TABLE_NAME = '").append(tableName).append("'  ")
                        .append("    AND (SELECT COUNT(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = kc.CONSTRAINT_NAME) = 1")
                        .append(" ORDER BY tc.CONSTRAINT_TYPE ASC, c.DATA_TYPE ASC");
                break;
            case ClickHouse:
                sql.append("SELECT name as column_name, type as column_type, 'PRI' as key_type")
                        .append(" FROM system.columns ")
                        .append(" WHERE database = ")
                        .append(schema == null ? "SELECT currentDatabase()) " : "'" + schema + "'")
                        .append(" AND table = '").append(tableName).append("'")
                        .append(" AND is_in_primary_key = 1")
                        .append(" ORDER BY type ASC");
                break;
            case Oracle:
                String normalizedSchema = schema == null ? username.toUpperCase() : schema.toUpperCase();
                // Preserve exact case if quoted, otherwise convert to uppercase
                String normalizedTableName = tableName.startsWith("\"") ? tableName : tableName.toUpperCase();

                sql.append("SELECT acc.column_name, upper(cc.data_type) AS COLUMN_TYPE, ")
                        .append(" CASE WHEN ac.constraint_type = 'P' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE ")
                        .append("FROM ")
                        .append("    all_constraints ac ")
                        .append("    JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name ")
                        .append("    JOIN all_tab_columns cc ON acc.table_name = cc.table_name AND acc.column_name = cc.column_name ")
                        .append("WHERE ")
                        .append("    ac.constraint_type IN ('P', 'U')  ")
                        .append("    AND ac.owner = '").append(normalizedSchema).append("' ")
                        .append("    AND acc.table_name = '").append(normalizedTableName).append("' ")
                        .append("    AND (SELECT COUNT(*) FROM all_cons_columns WHERE constraint_name = ac.constraint_name) = 1")
                        .append(" ORDER BY ac.constraint_type ASC, cc.data_type ASC");
                break;
            case SQLite:
                sql.append("SELECT ")
                        .append("    name AS column_name,  `type` AS column_type, 'PRI' AS KEY_TYPE ")
                        .append("FROM  pragma_table_info('")
                        .append(tableName).append("') ")
                        .append("WHERE   pk > 0 ")
                        .append("UNION ALL ")
                        .append("SELECT ")
                        .append(" t.name,  t.`type`, 'UNI' ")
                        .append("FROM pragma_index_list('").append(tableName).append("') AS il ")
                        .append("JOIN pragma_index_info(il.name) AS ii ")
                        .append("JOIN pragma_table_info('").append(tableName).append("') AS t ")
                        .append(" ON t.name = ii.name ")
                        .append("WHERE  il.`unique` = 1  AND il.origin != 'pk' ")
                        .append("GROUP BY seq HAVING  count(seq) = 1");
                break;
            default:
                LOG.warn("Unsupported database type: {}", dataBaseType);
                return null;
        }

        return sql.toString();
    }
}
