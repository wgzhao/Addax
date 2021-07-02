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

import com.alibaba.fastjson.JSON;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.plugin.rdbms.reader.Constant;
import com.wgzhao.datax.plugin.rdbms.reader.Key;
import com.wgzhao.datax.plugin.rdbms.util.DBUtil;
import com.wgzhao.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.wgzhao.datax.plugin.rdbms.util.DataBaseType;
import com.wgzhao.datax.plugin.rdbms.util.RdbmsException;
import com.wgzhao.datax.plugin.rdbms.util.RdbmsRangeSplitWrap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SingleTableSplitUtil
{
    private static final Logger LOG = LoggerFactory
            .getLogger(SingleTableSplitUtil.class);

    public static DataBaseType dataBaseType;

    private SingleTableSplitUtil()
    {
    }

    public static List<Configuration> splitSingleTable(
            Configuration configuration, int adviceNum)
    {
        List<Configuration> pluginParams = new ArrayList<>();
        List<String> rangeList;
        String splitPkName = configuration.getString(Key.SPLIT_PK);
        String column = configuration.getString(Key.COLUMN);
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);
        boolean hasWhere = StringUtils.isNotBlank(where);

        if (dataBaseType == DataBaseType.Oracle) {
            rangeList = genSplitSqlForOracle(splitPkName, table, where,
                    configuration, adviceNum);
            // warn: mysql etc to be added...
        }
        else {
            Pair<Object, Object> minMaxPK = getPkRange(configuration);
            if (null == minMaxPK) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "根据切分主键切分表失败. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }

            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
            if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
                // 切分后获取到的start/end 有 Null 的情况
                pluginParams.add(configuration);
                return pluginParams;
            }

            boolean isStringType = Constant.PK_TYPE_STRING.equals(configuration
                    .getString(Constant.PK_TYPE));
            boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration
                    .getString(Constant.PK_TYPE));

            if (isStringType) {
                rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                        String.valueOf(minMaxPK.getLeft()),
                        String.valueOf(minMaxPK.getRight()), adviceNum,
                        splitPkName, "'", dataBaseType);
            }
            else if (isLongType) {
                rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                        new BigInteger(minMaxPK.getLeft().toString()),
                        new BigInteger(minMaxPK.getRight().toString()),
                        adviceNum, splitPkName);
            }
            else {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }
        }
        String tempQuerySql;
        List<String> allQuerySql = new ArrayList<>();

        if (!rangeList.isEmpty()) {
            for (String range : rangeList) {
                Configuration tempConfig = configuration.clone();

                tempQuerySql = buildQuerySql(column, table, where)
                        + (hasWhere ? " and " : " where ") + range;

                allQuerySql.add(tempQuerySql);
                tempConfig.set(Key.QUERY_SQL, tempQuerySql);
                pluginParams.add(tempConfig);
            }
        }
        else {
            //pluginParams.add(configuration); // this is wrong for new & old split
            Configuration tempConfig = configuration.clone();
            tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere ? " and " : " where ")
                    + String.format(" %s IS NOT NULL", splitPkName);
            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }

        // deal pk is null
        Configuration tempConfig = configuration.clone();
        tempQuerySql = buildQuerySql(column, table, where)
                + (hasWhere ? " and " : " where ")
                + String.format(" %s IS NULL", splitPkName);

        allQuerySql.add(tempQuerySql);

        LOG.info("After split(), allQuerySql=[\n{}\n].",
                StringUtils.join(allQuerySql, "\n"));

        tempConfig.set(Key.QUERY_SQL, tempQuerySql);
        pluginParams.add(tempConfig);

        return pluginParams;
    }

    public static String buildQuerySql(String column, String table,
            String where)
    {
        String querySql;

        if (StringUtils.isBlank(where)) {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE_WITHOUT_WHERE,
                    column, table);
        }
        else {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE, column,
                    table, where);
        }

        return querySql;
    }

    @SuppressWarnings("resource")
    private static Pair<Object, Object> getPkRange(Configuration configuration)
    {
        String pkRangeSQL = genPKRangeSQL(configuration);

        int fetchSize = configuration.getInt(Constant.FETCH_SIZE);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String table = configuration.getString(Key.TABLE);

        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        Pair<Object, Object> minMaxPK = checkSplitPk(conn, pkRangeSQL, fetchSize, table, username, configuration);
        DBUtil.closeDBResources(null, null, conn);
        return minMaxPK;
    }

    public static void precheckSplitPk(Connection conn, String pkRangeSQL, int fetchSize,
            String table, String username)
    {
        Pair<Object, Object> minMaxPK = checkSplitPk(conn, pkRangeSQL, fetchSize, table, username, null);
        if (null == minMaxPK) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "根据切分主键切分表失败. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
        }
    }

    /**
     * 检测splitPk的配置是否正确。
     * configuration为null, 是precheck的逻辑，不需要回写PK_TYPE到configuration中
     *
     * @param conn database connection
     * @param pkRangeSQL query sql for getting the primary key range
     * @param fetchSize fetch size
     * @param table  the table name
     * @param username database connect username
     * @param configuration connect configuration
     * @return primary key range pair
     */
    private static Pair<Object, Object> checkSplitPk(Connection conn, String pkRangeSQL, int fetchSize, String table,
            String username, Configuration configuration)
    {
        LOG.info("split pk [sql={}] is running... ", pkRangeSQL);
        ResultSet rs = null;
        Pair<Object, Object> minMaxPK = null;
        try {
            String errorMsg = "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.";
            try {
                rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
            }
            catch (Exception e) {
                throw RdbmsException.asQueryException(dataBaseType, e, pkRangeSQL, table, username);
            }
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isPKTypeValid(rsMetaData)) {
                if (isStringType(rsMetaData.getColumnType(1))) {
                    if (configuration != null) {
                        configuration
                                .set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                    }
                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<>(
                                rs.getString(1), rs.getString(2));
                    }
                }
                else if (isLongType(rsMetaData.getColumnType(1))) {
                    if (configuration != null) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);
                    }

                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<>(
                                rs.getString(1), rs.getString(2));

                        // check: string shouldn't contain '.', for oracle
                        String minMax = rs.getString(1) + rs.getString(2);
                        if (StringUtils.contains(minMax, '.')) {
                            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, errorMsg);
                        }
                    }
                }
                else {
                    throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, errorMsg);
                }
            }
            else {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, errorMsg);
            }
        }
        catch (DataXException e) {
            throw e;
        }
        catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, "DataX尝试切分表发生错误. 请检查您的配置并作出修改.", e);
        }
        finally {
            DBUtil.closeDBResources(rs, null, null);
        }

        return minMaxPK;
    }

    private static boolean isPKTypeValid(ResultSetMetaData rsMetaData)
    {
        boolean ret = false;
        try {
            int minType = rsMetaData.getColumnType(1);
            int maxType = rsMetaData.getColumnType(2);

            boolean isNumberType = isLongType(minType);

            boolean isStringType = isStringType(minType);

            if (minType == maxType && (isNumberType || isStringType)) {
                ret = true;
            }
        }
        catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX获取切分主键(splitPk)字段类型失败. 该错误通常是系统底层异常导致. 请联系旺旺:askdatax或者DBA处理.");
        }
        return ret;
    }

    // warn: Types.NUMERIC is used for oracle! because oracle use NUMBER to
    // store INT, SMALLINT, INTEGER etc, and only oracle need to concern
    // Types.NUMERIC
    private static boolean isLongType(int type)
    {
        boolean isValidLongType = type == Types.BIGINT || type == Types.INTEGER
                || type == Types.SMALLINT || type == Types.TINYINT;

        if (SingleTableSplitUtil.dataBaseType == DataBaseType.Oracle) {
            isValidLongType |= type == Types.NUMERIC;
        }
        return isValidLongType;
    }

    private static boolean isStringType(int type)
    {
        return type == Types.CHAR || type == Types.NCHAR
                || type == Types.VARCHAR || type == Types.LONGVARCHAR
                || type == Types.NVARCHAR;
    }

    private static String genPKRangeSQL(Configuration configuration)
    {

        String splitPK = configuration.getString(Key.SPLIT_PK).trim();
        String table = configuration.getString(Key.TABLE).trim();
        String where = configuration.getString(Key.WHERE, null);
        return genPKSql(splitPK, table, where);
    }

    public static String genPKSql(String splitPK, String table, String where)
    {

        String minMaxTemplate = "SELECT MIN(%s),MAX(%s) FROM %s";
        String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK,
                table);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)",
                    pkRangeSQL, where, splitPK);
        }
        return pkRangeSQL;
    }

    /**
     * support Number and String split
     *
     *
     * @param splitPK primary key will be splitted
     * @param table table name
     * @param where where clause
     * @param configuration configuration
     * @param adviceNum the number of split
     * @return list of string
     */
    public static List<String> genSplitSqlForOracle(String splitPK,
            String table, String where, Configuration configuration,
            int adviceNum)
    {
        if (adviceNum < 1) {
            throw new IllegalArgumentException(String.format(
                    "切分份数不能小于1. 此处:adviceNum=[%s].", adviceNum));
        }
        else if (adviceNum == 1) {
            return Collections.emptyList();
        }
        String whereSql = String.format("%s IS NOT NULL", splitPK);
        if (StringUtils.isNotBlank(where)) {
            whereSql = String.format(" WHERE (%s) AND (%s) ", whereSql, where);
        }
        else {
            whereSql = String.format(" WHERE (%s) ", whereSql);
        }
        Double percentage = configuration.getDouble(Key.SAMPLE_PERCENTAGE, 0.1);
        String sampleSqlTemplate = "SELECT * FROM ( SELECT %s FROM %s SAMPLE (%s) %s ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= %s ORDER by %s ASC";
        String splitSql = String.format(sampleSqlTemplate, splitPK, table,
                percentage, whereSql, adviceNum, splitPK);

        int fetchSize = configuration.getInt(Constant.FETCH_SIZE, 32);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL,
                username, password);
        LOG.info("split pk [sql={}] is running... ", splitSql);
        ResultSet rs = null;
        List<Pair<Object, Integer>> splitedRange = new ArrayList<>();
        try {
            try {
                rs = DBUtil.query(conn, splitSql, fetchSize);
            }
            catch (Exception e) {
                throw RdbmsException.asQueryException(dataBaseType, e,
                        splitSql, table, username);
            }
            configuration
                    .set(Constant.PK_TYPE, Constant.PK_TYPE_MONTECARLO);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            while (DBUtil.asyncResultSetNext(rs)) {
                ImmutablePair<Object, Integer> eachPoint = new ImmutablePair<>(
                        rs.getObject(1), rsMetaData.getColumnType(1));
                splitedRange.add(eachPoint);
            }
        }
        catch (DataXException e) {
            throw e;
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX尝试切分表发生错误. 请检查您的配置并作出修改.", e);
        }
        finally {
            DBUtil.closeDBResources(rs, null, null);
        }
        LOG.debug(JSON.toJSONString(splitedRange));
        List<String> rangeSql = new ArrayList<>();
        int splitedRangeSize = splitedRange.size();
        // warn: splitedRangeSize may be 0 or 1，切分规则为IS NULL以及 IS NOT NULL
        // demo: Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[24999930].
        if (splitedRangeSize >= 2) {
            // warn: oracle Number is long type here
            if (isLongType(splitedRange.get(0).getRight())) {
                BigInteger[] integerPoints = new BigInteger[splitedRange.size()];
                for (int i = 0; i < splitedRangeSize; i++) {
                    integerPoints[i] = new BigInteger(splitedRange.get(i)
                            .getLeft().toString());
                }
                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(integerPoints,
                        splitPK));
                // its ok if splitedRangeSize is 1
                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(
                        integerPoints[0], integerPoints[splitedRangeSize - 1],
                        splitPK));
            }
            else if (isStringType(splitedRange.get(0).getRight())) {
                // warn: treated as string type
                String[] stringPoints = new String[splitedRange.size()];
                for (int i = 0; i < splitedRangeSize; i++) {
                    stringPoints[i] = splitedRange.get(i).getLeft()
                            .toString();
                }
                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(stringPoints,
                        splitPK, "'", dataBaseType));
                // its ok if splitedRangeSize is 1
                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(
                        stringPoints[0], stringPoints[splitedRangeSize - 1],
                        splitPK, "'", dataBaseType));
            }
            else {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                                "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
            }
        }
        return rangeSql;
    }

    /**
     * 尝试自动获取指定表的主键，如果有多个，则取第一个
     *
     * @param connection JDBC 连接串
     * @param table 要查询的表
     * @return 主键
     */
    public static String getPrimaryKey(Connection connection, String table)
            throws SQLException
    {
        String sql;
        if (dataBaseType == DataBaseType.Oracle) {
            return getOraclePrimaryKey(connection, table);
        } else {
            sql = getPrimaryKeyQuery(table);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                sql =  resultSet.getString(0);
            }
        }
        return sql;
    }

    /**
     * 依据不同数据库类型，返回对应的获取主键的SQL语句
     *
     * @param tableName 要查询的表
     * @return 获取主键 SQL 语句
     */
    public static String getPrimaryKeyQuery(String tableName)
    {
        String sql = null;
        switch (dataBaseType) {
            case DataBaseType.MySql:
                sql = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_SCHEMA = SELECT SCHEMA() "
                        + "AND TABLE_NAME = '"+tableName+"' "
                        + "AND COLUMN_KEY = 'PRI'";
                break;

        }
        return sql;
    }

    public static String getOraclePrimaryKey(Connection connection, String tableName)
    {
        PreparedStatement pStmt = null;
        ResultSet rset = null;
        List<String> columns = new ArrayList<String>();

        String tableOwner = null;
        String shortTableName = tableName;
        int qualifierIndex = tableName.indexOf('.');
        if (qualifierIndex != -1) {
            tableOwner = tableName.substring(0, qualifierIndex);
            shortTableName = tableName.substring(qualifierIndex + 1);
        }

        try {
            conn = getConnection();

            if (tableOwner == null) {
                tableOwner = getSessionUser(conn);
            }

            pStmt = conn.prepareStatement(QUERY_PRIMARY_KEY_FOR_TABLE,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pStmt.setString(1, shortTableName);
            pStmt.setString(2, tableOwner);
            rset = pStmt.executeQuery();

            while (rset.next()) {
                columns.add(rset.getString(1));
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Failed to rollback transaction", ex);
            }
            LoggingUtils.logAll(LOG, "Failed to list columns", e);
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close resultset", ex);
                }
            }
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException ex) {
                    LoggingUtils.logAll(LOG, "Failed to close statement", ex);
                }
            }

            try {
                close();
            } catch (SQLException ex) {
                LoggingUtils.logAll(LOG, "Unable to discard connection", ex);
            }
        }

        if (columns.size() == 0) {
            // Table has no primary key
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
}