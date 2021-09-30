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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DBUtilErrorCode;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.alibaba.druid.sql.parser.ParserException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class WriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    private WriterUtil() {}

    public static List<Configuration> doSplit(Configuration simplifiedConf, int adviceNumber)
    {

        List<Configuration> splitResultConfigs = new ArrayList<>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw AddaxException.asAddaxException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Key.CONNECTION, Object.class);

        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
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
        }

        return splitResultConfigs;
    }

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName)
    {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }

        List<String> renderedSqls = new ArrayList<>();
        for (String sql : preOrPostSqls) {
            //preSql为空时，不加入执行队列
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage, DataBaseType dataBaseType)
    {
        String currentSql = null;
        try (Statement stmt = conn.createStatement()){
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
            throw AddaxException.asAddaxException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 目前仅支持replace,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        }
        String writeDataSqlTemplate;
        if (forceUseUpdate || mode.startsWith("update")) {
            if (dataBaseType == DataBaseType.MySql) {
                writeDataSqlTemplate = "INSERT INTO %s (" + columns + ") VALUES(" + placeHolders + ")" +
                        doMysqlUpdate(columnHolders);
            }
            else if (dataBaseType == DataBaseType.Oracle) {
                writeDataSqlTemplate = doOracleUpdate(writeMode, columnHolders, valueHolders) +
                        "INSERT (" + columns + ") VALUES ( " + placeHolders + " )";
            }
            else if (dataBaseType == DataBaseType.PostgreSQL) {
                writeDataSqlTemplate = "INSERT INTO %s (" + columns + ") VALUES ( " + placeHolders + " )" +
                        doPostgresqlUpdate(writeMode, columnHolders);
            }
            else {
                throw AddaxException.asAddaxException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("当前数据库不支持 writeMode:%s 模式.", writeMode));
            }
        }
        else {
            //这里是保护,如果其他错误的使用了update,需要更换为replace
            if (mode.startsWith("update")) {
                writeMode = "replace";
            }
            writeDataSqlTemplate = writeMode + " INTO %s ( " + columns + ") VALUES ( " + placeHolders + " )";
        }

        return writeDataSqlTemplate;
    }


    private static String doPostgresqlUpdate(String writeMode, List<String> columnHolders)
    {
        String conflict = writeMode.replace("update", "");
        StringBuilder sb = new StringBuilder();
        sb.append(" ON CONFLICT ");
        sb.append(conflict);
        sb.append(" DO ");
        if (columnHolders == null || columnHolders.size() < 1) {
            sb.append("NOTHING");
            return sb.toString();
        }
        sb.append(" UPDATE SET ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            } else {
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

    public static String doOracleUpdate(String merge, List<String> columnHolders, List<String> valueHolders)
    {
        String[] sArray = getStrings(merge);
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO %s A USING ( SELECT ");

        boolean first = true;
        boolean first1 = true;
        StringBuilder str = new StringBuilder();
        StringBuilder update = new StringBuilder();
        for (String columnHolder : columnHolders) {
            if (Arrays.asList(sArray).contains(columnHolder)) {
                if (!first) {
                    sb.append(",");
                    str.append(" AND ");
                }
                else {
                    first = false;
                }
                str.append("TMP.").append(columnHolder);
                sb.append("?");
                str.append(" = ");
                sb.append(" AS ");
                str.append("A.").append(columnHolder);
                sb.append(columnHolder);
            }
        }

        for (String columnHolder : columnHolders) {
            if (!Arrays.asList(sArray).contains(columnHolder)) {
                if (!first1) {
                    update.append(",");
                }
                else {
                    first1 = false;
                }
                update.append(columnHolder);
                update.append(" = ");
                update.append("?");
            }
        }

        sb.append(" FROM DUAL ) TMP ON (");
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
        List<Object> conns = originalConfig.getList(Key.CONNECTION, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
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
        List<Object> conns = originalConfig.getList(Key.CONNECTION, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
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
