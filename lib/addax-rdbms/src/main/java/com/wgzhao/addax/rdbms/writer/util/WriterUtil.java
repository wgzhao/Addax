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

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

public final class WriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    private WriterUtil() {}

    public static List<Configuration> doSplit(Configuration simplifiedConf, int adviceNumber)
    {

        List<Configuration> splitResultConfigs = new ArrayList<>();

        int tableNumber = simplifiedConf.getInt(Key.TABLE_NUMBER);

        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
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
        String conflict = writeMode.replaceFirst("update", "");
        StringBuilder sb = new StringBuilder();
        sb.append(" ON CONFLICT ");
        sb.append(conflict);
        sb.append(" DO ");
        if (columnHolders == null || columnHolders.isEmpty()) {
            sb.append("NOTHING");
            return sb.toString();
        }
        sb.append(" UPDATE SET ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            }
            else {
                first = false;
            }
            sb.append(column);
            sb.append("=excluded.");
            sb.append(column);
        }
        return sb.toString();
    }

    public static String doMysqlUpdate(List<String> columnHolders)
    {
        if (columnHolders == null || columnHolders.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" ON DUPLICATE KEY UPDATE ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            }
            else {
                first = false;
            }
            sb.append(column);
            sb.append("=VALUES(");
            sb.append(column);
            sb.append(")");
        }

        return sb.toString();
    }

    public static String doOracleOrSqlServerUpdate(String merge, List<String> columnHolders, List<String> valueHolders, DataBaseType dataBaseType)
    {
        String[] sArray = getStrings(merge);
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO %s A USING ( SELECT ");

        boolean first = true;
        boolean first1 = true;
        StringBuilder str = new StringBuilder();
        StringBuilder update = new StringBuilder();
        for (int i = 0; i < columnHolders.size(); i++) {
            String columnHolder = columnHolders.get(i);
            if (Arrays.stream(sArray).anyMatch(s -> s.equalsIgnoreCase(columnHolder))) {
                if (!first) {
                    sb.append(",");
                    str.append(" AND ");
                }
                else {
                    first = false;
                }
                str.append("TMP.").append(columnHolder);
                sb.append(valueHolders.get(i));
                str.append(" = ");
                sb.append(" AS ");
                str.append("A.").append(columnHolder);
                sb.append(columnHolder);
            }
        }

        for (int i = 0; i < columnHolders.size(); i++) {
            String columnHolder = columnHolders.get(i);
            if (Arrays.stream(sArray).noneMatch(s -> s.equalsIgnoreCase(columnHolder))) {
                if (!first1) {
                    update.append(",");
                }
                else {
                    first1 = false;
                }
                update.append(columnHolder);
                update.append(" = ");
                update.append(valueHolders.get(i));
            }
        }

        if (dataBaseType == DataBaseType.Oracle) {
            sb.append(" FROM DUAL ) TMP ON (");
        }
        else {
            sb.append(" ) TMP ON (");
        }
        sb.append(str);
        sb.append(" ) WHEN MATCHED THEN UPDATE SET ");
        sb.append(update);
        sb.append(" WHEN NOT MATCHED THEN ");
        return sb.toString();
    }

    public static String[] getStrings(String merge)
    {
        merge = merge.replace("update", "");
        merge = merge.replace("(", "");
        merge = merge.replace(")", "");
        merge = merge.replace(" ", "");
        return merge.split(",");
    }

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
