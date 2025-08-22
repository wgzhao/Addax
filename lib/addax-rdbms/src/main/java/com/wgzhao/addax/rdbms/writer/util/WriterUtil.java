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

package com.wgzhao.addax.rdbms.writer.util;

import com.alibaba.druid.sql.parser.ParserException;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

/**
 * Utility class for RDBMS writer operations.
 * Provides methods for splitting configurations, rendering SQL templates,
 * executing SQL statements, and generating write templates based on database type and write mode.
 */
public final class WriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    private WriterUtil() {}

    /**
     * Split writer configuration into slice configs according to table list and channel number.
     * If only one table, replicate config for each channel; if multi-table, require 1:1 with channels.
     *
     * @param simplifiedConf The simplified configuration containing table and connection information
     * @param adviceNumber The number of channels (advised number of parallel tasks)
     * @return List of configuration slices, one for each channel
     */
    public static List<Configuration> doSplit(Configuration simplifiedConf, int adviceNumber)
    {

        List<Configuration> splitResultConfigs = new ArrayList<>();

        int tableNumber = simplifiedConf.getInt(Key.TABLE_NUMBER);

        if (tableNumber == 1) {
            // Since table and jdbcUrl have been extracted in the previous master prepare phase, processing here is simple
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The number of writing tables(" + tableNumber + ") is NOT equal to the number of channels(" + adviceNumber + ")."
            );
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        Configuration connConf = simplifiedConf.getConfiguration(Key.CONNECTION);

        Configuration sliceConfig = simplifiedConf.clone();

        jdbcUrl = connConf.getString(Key.JDBC_URL);
        sliceConfig.set(Key.JDBC_URL, jdbcUrl);

        sliceConfig.remove(Key.CONNECTION);

        List<String> tables = connConf.getList(Key.TABLE, String.class);

        for (String table : tables) {
            Configuration tempSlice = sliceConfig.clone();
            tempSlice.set(Key.TABLE, table);
            tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
            tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

            splitResultConfigs.add(tempSlice);
        }

        return splitResultConfigs;
    }

    /**
     * Render pre- / post-SQL templates by replacing table placeholder with real table name.
     *
     * @param preOrPostSqls List of SQL templates that may contain table placeholders
     * @param tableName The actual table name to replace placeholders with
     * @return List of rendered SQL statements with placeholders replaced
     */
    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName)
    {
        if (null == preOrPostSqls) {
            return new ArrayList<>();
        }

        List<String> renderedSqls = new ArrayList<>();
        for (String sql : preOrPostSqls) {
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    /**
     * Execute a list of SQL statements in order, fail fast on first error.
     *
     * @param conn Database connection to execute statements on
     * @param sqls List of SQL statements to execute in sequence
     */
    public static void executeSqls(Connection conn, List<String> sqls)
    {
        String currentSql = null;
        try (Statement stmt = conn.createStatement()) {
            for (String sql : sqls) {
                currentSql = sql;
                stmt.execute(sql);
            }
        }
        catch (Exception e) {
            throw RdbmsException.asQueryException(e, currentSql);
        }
    }

    /**
     * Build upsert/insert template based on writeMode and database dialect.
     *
     * @param columnHolders List of column names for the INSERT/UPDATE statement
     * @param valueHolders List of value placeholders (typically "?") for parameters
     * @param writeMode The write mode (insert, replace, update, etc.)
     * @param dataBaseType The database type for generating appropriate SQL syntax
     * @param forceUseUpdate Whether to force using UPDATE syntax regardless of writeMode
     * @return SQL template string with placeholders for table name and values
     */
    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders,
            String writeMode, DataBaseType dataBaseType, boolean forceUseUpdate)
    {
        String mode = writeMode.trim().toLowerCase();
        String columns = StringUtils.join(columnHolders, ",");
        String placeHolders = StringUtils.join(valueHolders, ",");
        boolean isWriteModeLegal = mode.startsWith("insert") || mode.startsWith("replace") || mode.startsWith("update");

        if (!isWriteModeLegal) {
            throw AddaxException.illegalConfigValue(Key.WRITE_MODE, mode);
        }
        String writeDataSqlTemplate;
        if (forceUseUpdate || mode.startsWith("update")) {
            if (dataBaseType == DataBaseType.MySql) {
                writeDataSqlTemplate = "INSERT INTO %s (" + columns + ") VALUES(" + placeHolders + ")" +
                        doMysqlUpdate(columnHolders);
            }
            else if (dataBaseType == DataBaseType.Oracle) {
                writeDataSqlTemplate = doOracleOrSqlServerUpdate(writeMode, columnHolders, valueHolders, dataBaseType) +
                        "INSERT (" + columns + ") VALUES ( " + placeHolders + " )";
            }
            else if (dataBaseType == DataBaseType.PostgreSQL) {
                writeDataSqlTemplate = "INSERT INTO %s (" + columns + ") VALUES ( " + placeHolders + " )" +
                        doPostgresqlUpdate(writeMode, columnHolders);
            }
            else if (dataBaseType == DataBaseType.SQLServer) {
                writeDataSqlTemplate = doOracleOrSqlServerUpdate(writeMode, columnHolders, valueHolders, dataBaseType) +
                        "INSERT (" + columns + ") VALUES ( " + placeHolders + " );";
            }
            else {
                throw AddaxException.illegalConfigValue(Key.WRITE_MODE, writeMode);
            }
        }
        else {
            if (mode.startsWith("update")) {
                writeMode = "replace";
            }
            writeDataSqlTemplate = writeMode + " INTO %s ( " + columns + ") VALUES ( " + placeHolders + " )";
        }

        return writeDataSqlTemplate;
    }

    private static String doPostgresqlUpdate(String writeMode, List<String> columnHolders)
    {
        String conflict = writeMode.replaceFirst("update", "").trim();
        StringBuilder sb = new StringBuilder();
        sb.append(" ON CONFLICT ").append(conflict).append(" DO ");

        String conflictWithoutParentheses = conflict.substring(1, conflict.length() - 1).trim();
        List<String> conflictColumns = Arrays.stream(conflictWithoutParentheses.split(","))
                .map(String::trim)
                .toList();

        if (columnHolders == null || columnHolders.isEmpty()) {
            sb.append("NOTHING");
        }
        else {
            sb.append("UPDATE SET ");
            sb.append(columnHolders.stream()
                    .filter(column -> !conflictColumns.contains(column))
                    .map(column -> column + "=excluded." + column)
                    .collect(Collectors.joining(",")));
        }

        return sb.toString();
    }

    /**
     * Generates MySQL-specific ON DUPLICATE KEY UPDATE clause for upsert operations.
     *
     * @param columnHolders List of column names to include in the UPDATE clause
     * @return MySQL ON DUPLICATE KEY UPDATE clause string
     */
    public static String doMysqlUpdate(List<String> columnHolders)
    {
        if (columnHolders == null || columnHolders.isEmpty()) {
            return "";
        }

        String updates = columnHolders.stream()
                .map(column -> column + "=VALUES(" + column + ")")
                .collect(Collectors.joining(","));

        return " ON DUPLICATE KEY UPDATE " + updates;
    }

    /**
     * Generates Oracle or SQL Server MERGE statement for upsert operations.
     *
     * @param merge The merge configuration containing key columns for matching
     * @param columnHolders List of all column names in the target table
     * @param valueHolders List of value placeholders for the MERGE statement
     * @param dataBaseType The database type (Oracle or SQL Server)
     * @return MERGE statement template with placeholders
     */
    public static String doOracleOrSqlServerUpdate(String merge, List<String> columnHolders, List<String> valueHolders, DataBaseType dataBaseType)
    {
        // Extract key columns from the merge clause for the join condition
        String[] keyColumns = getStrings(merge);

        if (columnHolders == null || columnHolders.isEmpty() || valueHolders == null || valueHolders.isEmpty()) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Column holders or value holders cannot be empty for MERGE operation");
        }

        StringBuilder mergeSql = new StringBuilder();
        StringBuilder joinCondition = new StringBuilder();
        StringBuilder updateClause = new StringBuilder();

        // Start building MERGE statement
        mergeSql.append("MERGE INTO %s A USING (SELECT ");

        // Build SELECT clause and join conditions
        boolean isFirstKeyColumn = true;
        for (int i = 0; i < columnHolders.size(); i++) {
            String columnHolder = columnHolders.get(i);
            // If this is a key column used for matching records
            if (Arrays.stream(keyColumns).anyMatch(s -> s.equalsIgnoreCase(columnHolder))) {
                if (!isFirstKeyColumn) {
                    mergeSql.append(", ");
                    joinCondition.append(" AND ");
                }
                else {
                    isFirstKeyColumn = false;
                }

                mergeSql.append(valueHolders.get(i)).append(" AS ").append(columnHolder);
                joinCondition.append("TMP.").append(columnHolder).append(" = A.").append(columnHolder);
            }
        }

        // Add FROM clause based on database type
        if (dataBaseType == DataBaseType.Oracle) {
            mergeSql.append(" FROM DUAL");
        }
        mergeSql.append(") TMP ON (").append(joinCondition).append(")");

        // Build UPDATE SET clause for non-key columns
        boolean isFirstUpdateColumn = true;
        for (int i = 0; i < columnHolders.size(); i++) {
            String columnHolder = columnHolders.get(i);
            if (Arrays.stream(keyColumns).noneMatch(s -> s.equalsIgnoreCase(columnHolder))) {
                if (!isFirstUpdateColumn) {
                    updateClause.append(", ");
                }
                else {
                    isFirstUpdateColumn = false;
                }
                updateClause.append(columnHolder).append(" = ").append(valueHolders.get(i));
            }
        }

        // Complete the MERGE statement
        mergeSql.append(" WHEN MATCHED THEN UPDATE SET ").append(updateClause);
        mergeSql.append(" WHEN NOT MATCHED THEN ");

        return mergeSql.toString();
    }

    /**
     * Parses merge configuration string to extract key column names.
     *
     * @param merge The merge configuration string containing column names
     * @return Array of key column names extracted from the merge string
     */
    public static String[] getStrings(String merge)
    {
        merge = merge.replace("update", "");
        merge = merge.replace("(", "");
        merge = merge.replace(")", "");
        merge = merge.replace(" ", "");
        return merge.split(",");
    }

    /**
     * Validates the syntax of pre-SQL statements before execution.
     *
     * @param originalConfig The configuration containing pre-SQL statements
     * @param type The database type for SQL validation
     */
    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type)
    {
        Configuration connConf = originalConfig.getConfiguration(Key.CONNECTION);
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);
        List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);

        if (!renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].", StringUtils.join(renderedPreSqls, ";"));
            for (String sql : renderedPreSqls) {
                try {
                    DBUtil.sqlValid(sql, type);
                }
                catch (ParserException e) {
                    throw RdbmsException.asPreSQLParserException(e, sql);
                }
            }
        }
    }

    /**
     * Validates the syntax of post-SQL statements before execution.
     *
     * @param originalConfig The configuration containing post-SQL statements
     * @param type The database type for SQL validation
     */
    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type)
    {
        Configuration connConf = originalConfig.getConfiguration(Key.CONNECTION);
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> postSqls = originalConfig.getList(Key.POST_SQL, String.class);
        List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);
        if (!renderedPostSqls.isEmpty()) {
            LOG.info("Begin to preCheck postSqls:[{}].", StringUtils.join(renderedPostSqls, ";"));
            for (String sql : renderedPostSqls) {
                DBUtil.sqlValid(sql, type);
            }
        }
    }
}

