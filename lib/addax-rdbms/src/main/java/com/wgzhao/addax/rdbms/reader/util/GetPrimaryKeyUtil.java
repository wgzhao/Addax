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

        String sql;

        switch (dataBaseType) {
            case MySql:
                sql = String.format("""
                         select
                         c.COLUMN_NAME, upper(c.DATA_TYPE) AS COLUMN_TYPE, c.COLUMN_KEY AS KEY_TYPE
                         from INFORMATION_SCHEMA.`COLUMNS` c , INFORMATION_SCHEMA.STATISTICS s
                         where c.TABLE_SCHEMA = s.TABLE_SCHEMA
                          AND c.TABLE_NAME = s.TABLE_NAME
                          AND c.COLUMN_NAME = s.COLUMN_NAME
                          AND s.TABLE_SCHEMA = %s
                          AND s.TABLE_NAME = '%s'
                          AND NON_UNIQUE = 0
                         AND COLUMN_KEY <> 'MUL' and COLUMN_KEY <> ''
                         ORDER BY c.COLUMN_KEY ASC, c.DATA_TYPE ASC
                        """, schema == null ? "(SELECT SCHEMA()) " : "'" + schema + "'", tableName);
                break;
            case PostgreSQL:
                sql = String.format("""
                         SELECT a.attname AS COLUMN_NAME,
                         upper(format_type(a.atttypid, a.atttypmod)) AS COLUMN_TYPE,
                         CASE WHEN con.contype = 'p' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE
                         FROM pg_constraint con
                         JOIN pg_class rel ON rel.oid = con.conrelid
                         JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                         LEFT JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = con.conrelid
                         WHERE nsp.nspname = %s
                         AND rel.relname = '%s'
                         AND con.contype IN ('p', 'u') AND array_length(con.conkey, 1) = 1
                         ORDER BY con.contype ASC, a.atttypid ASC
                        """, schema == null ? "(SELECT CURRENT_SCHEMA()) " : "'" + schema + "'", tableName);
                break;
            case SQLServer:
                sql = String.format("""
                        SELECT  kc.COLUMN_NAME, upper(c.DATA_TYPE) AS COLUMN_TYPE,
                            CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE
                         FROM
                            INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
                            JOIN INFORMATION_SCHEMA.COLUMNS c ON kc.TABLE_NAME = c.TABLE_NAME AND kc.COLUMN_NAME = c.COLUMN_NAME
                         WHERE
                            tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
                            AND kc.TABLE_SCHEMA = %s
                            AND kc.TABLE_NAME = '%s'
                            AND (SELECT COUNT(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = kc.CONSTRAINT_NAME) = 1
                         ORDER BY tc.CONSTRAINT_TYPE ASC, c.DATA_TYPE ASC
                        """, schema == null ? "(select schema_name())  " : "'" + schema + "'", tableName);
                break;
            case ClickHouse:
                sql = String.format("""
                        SELECT name as column_name, type as column_type, 'PRI' as key_type
                        FROM system.columns
                         WHERE database =  %s
                         AND table = '%s'
                         AND is_in_primary_key = 1
                         ORDER BY type ASC
                        """, schema == null ? "SELECT currentDatabase()) " : "'" + schema + "'", tableName);
                break;
            case Oracle:
                String normalizedSchema = schema == null ? username.toUpperCase() : schema.toUpperCase();
                // Preserve exact case if quoted, otherwise convert to uppercase
                String normalizedTableName = tableName.startsWith("\"") ? tableName : tableName.toUpperCase();

                sql = String.format("""
                        SELECT acc.column_name, upper(cc.data_type) AS COLUMN_TYPE,
                        CASE WHEN ac.constraint_type = 'P' THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE
                        FROM
                            all_constraints ac
                            JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name
                            JOIN all_tab_columns cc ON acc.table_name = cc.table_name AND acc.column_name = cc.column_name
                        WHERE
                            ac.constraint_type IN ('P', 'U')
                            AND ac.owner = '%s'
                            AND acc.table_name = '%s'
                            AND (SELECT COUNT(*) FROM all_cons_columns WHERE constraint_name = ac.constraint_name) = 1
                        ORDER BY ac.constraint_type ASC, cc.data_type ASC
                        """, normalizedSchema, normalizedTableName);
                break;
            case SQLite:
                sql = String.format("""
                        SELECT name AS column_name,  `type` AS column_type, 'PRI' AS KEY_TYPE
                        FROM  pragma_table_info('%1$s')
                        WHERE  pk > 0
                        UNION ALL
                        SELECT t.name,  t.`type`, 'UNI'
                        FROM pragma_index_list('%1$s') AS il
                        JOIN pragma_index_info(il.name) AS ii
                        JOIN pragma_table_info('%1$s') AS t
                         ON t.name = ii.name
                        WHERE  il.`unique` = 1  AND il.origin != 'pk'
                        GROUP BY seq HAVING  count(seq) = 1
                        """, tableName);
                break;
            case Sybase:
                sql = String.format("""
                        SELECT  c.name AS COLUMN_NAME, UPPER(t.name) AS COLUMN_TYPE,  CASE WHEN i.status & 2048 = 2048 THEN 'PRI' ELSE 'UNI' END AS KEY_TYPE
                        FROM sysindexes i JOIN syscolumns c ON i.id = c.id AND
                        c.colid = (
                                SELECT MIN(cx.colid) FROM sysindexkeys k
                                JOIN syscolumns cx ON k.id = cx.id AND k.colid = cx.colid
                                WHERE k.id = i.id AND k.indid = i.indid
                                )
                            JOIN sysobjects o ON i.id = o.id
                            JOIN systypes t ON c.usertype = t.usertype
                        WHERE
                            o.name = '%s'
                            AND (i.status & 2 = 2 OR i.status & 2048 = 2048) ")
                            AND (SELECT COUNT(*) FROM sysindexkeys k WHERE k.id = i.id AND k.indid = i.indid) = 1 ")
                            AND o.uid = USER_ID('%s')
                        ORDER BY
                            CASE WHEN i.status & 2048 = 2048 THEN 0 ELSE 1 END,  t.name
                        """, tableName, username != null ? username : schema);
                break;
            default:
                LOG.warn("Unsupported database type: {}", dataBaseType);
                return null;
        }
        return sql;
    }
}
