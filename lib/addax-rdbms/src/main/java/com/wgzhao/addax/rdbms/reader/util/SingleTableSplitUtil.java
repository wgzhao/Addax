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

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.RdbmsException;
import com.wgzhao.addax.rdbms.util.RdbmsRangeSplitWrap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.NOT_SUPPORT_TYPE;

public class SingleTableSplitUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(SingleTableSplitUtil.class);

    public static DataBaseType dataBaseType;

    private SingleTableSplitUtil()
    {
    }

    public static List<Configuration> splitSingleTable(Configuration configuration, int adviceNum)
    {
        List<Configuration> pluginParams = new ArrayList<>();
        List<String> rangeList;
        String splitPkName = configuration.getString(Key.SPLIT_PK);
        String column = configuration.getString(Key.COLUMN);
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);
        boolean hasWhere = StringUtils.isNotBlank(where);
        if (dataBaseType == DataBaseType.Oracle) {
            rangeList = genSplitSqlForOracle(splitPkName, table, where, configuration, adviceNum);
            // warn: mysql etc to be added...
        }
        else {
            Pair<Object, Object> minMaxPK = getPkRange(configuration);
            if (null == minMaxPK) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Primary key-based table splitting failed. The key type ONLY supports integer and string.");
            }

            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
            if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
                // 切分后获取到的start/end 有 Null 的情况
                pluginParams.add(configuration);
                return pluginParams;
            }

            boolean isStringType = Constant.PK_TYPE_STRING.equals(configuration.getString(Constant.PK_TYPE));
            boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration.getString(Constant.PK_TYPE));

            if (isStringType) {
                rangeList = splitStringPk(configuration, table, where, minMaxPK.getLeft().toString(), minMaxPK.getRight().toString(),
                        adviceNum, splitPkName);
            }
            else if (isLongType) {
                rangeList = RdbmsRangeSplitWrap.splitAndWrap(new BigInteger(minMaxPK.getLeft().toString()),
                        new BigInteger(minMaxPK.getRight().toString()), adviceNum, splitPkName);
            }
            else {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "the splitPk[" + splitPkName + "] type is unsupported, it only support int and string");
            }
        }
        String tempQuerySql;
        List<String> allQuerySql = new ArrayList<>();

        if (!rangeList.isEmpty()) {
            for (String range : rangeList) {
                Configuration tempConfig = configuration.clone();

                tempQuerySql = buildQuerySql(column, table, where) + (hasWhere ? " AND " : " WHERE ") + range;

                allQuerySql.add(tempQuerySql);
                tempConfig.set(Key.QUERY_SQL, tempQuerySql);
                pluginParams.add(tempConfig);
            }
        }
        else {
            //pluginParams.add(configuration); // this is wrong for new & old split
            Configuration tempConfig = configuration.clone();
            tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere ? " AND " : " WHERE ")
                    + String.format(" %s IS NOT NULL", splitPkName);
            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }

        // if the `where` clause contains splitPkName, it means that the splitPkName is not null
        // deal pk is null
        if (where == null  || ! where.contains(splitPkName)) {
            Configuration tempConfig = configuration.clone();
            tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere? " AND ": " WHERE ")
                    + splitPkName + " IS NULL";
            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }
        LOG.info("After splitting, all query sql = [\n{}\n].", StringUtils.join(allQuerySql, "\n"));

        return pluginParams;
    }

    public static String buildQuerySql(String column, String table, String where)
    {
        String querySql;

        if (StringUtils.isBlank(where)) {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE_WITHOUT_WHERE, column, table);
        }
        else {
            querySql = String.format(Constant.QUERY_SQL_TEMPLATE, column, table, where);
        }

        return querySql;
    }

    @SuppressWarnings("resource")
    private static Pair<Object, Object> getPkRange(Configuration configuration)
    {
        String pkRangeSQL = genPKRangeSQL(configuration);

        int fetchSize = configuration.getInt(Key.FETCH_SIZE);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String table = configuration.getString(Key.TABLE);

        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        Pair<Object, Object> minMaxPK = checkSplitPk(conn, pkRangeSQL, fetchSize, table, username, configuration);
        DBUtil.closeDBResources(null, conn);
        return minMaxPK;
    }

    public static void preCheckSplitPk(Connection conn, String pkRangeSQL, int fetchSize,
            String table, String username)
    {
        Pair<Object, Object> minMaxPK = checkSplitPk(conn, pkRangeSQL, fetchSize, table, username, null);
        if (null == minMaxPK) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The split key should be single column, and the type is either integer or string.");
        }
    }

    /**
     * 检测splitPk的配置是否正确。
     * configuration为null, 是preCheck的逻辑，不需要回写PK_TYPE到configuration中
     *
     * @param conn database connection
     * @param pkRangeSQL query sql for getting the primary key range
     * @param fetchSize fetch size
     * @param table the table name
     * @param username database connect username
     * @param configuration connect configuration
     * @return primary key range pair
     */
    private static Pair<Object, Object> checkSplitPk(Connection conn, String pkRangeSQL, int fetchSize, String table,
            String username, Configuration configuration)
    {
        ResultSet rs = null;
        Pair<Object, Object> minMaxPK = null;
        try {
            String errorMsg =  "the splitPk type is unsupported, it only support int and string";
            try {
                rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
            }
            catch (Exception e) {
                throw RdbmsException.asQueryException(e, pkRangeSQL);
            }
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isPKTypeValid(rsMetaData)) {
                if (isStringType(rsMetaData.getColumnType(1))) {
                    if (configuration != null) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                    }
                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<>(rs.getString(1), rs.getString(2));
                    }
                }
                else if (isLongType(rsMetaData.getColumnType(1))) {
                    if (configuration != null) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);
                    }

                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<>(rs.getString(1), rs.getString(2));

                        // check: string shouldn't contain '.', for oracle
                        String minMax = rs.getString(1) + rs.getString(2);
                        if (StringUtils.contains(minMax, '.')) {
                            throw AddaxException.asAddaxException(CONFIG_ERROR, errorMsg);
                        }
                    }
                }
                else {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, errorMsg);
                }
            }
            else {
                throw AddaxException.asAddaxException(CONFIG_ERROR, errorMsg);
            }
        }
        catch (AddaxException e) {
            throw e;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to split the table.", e);
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
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "Failed to obtain the type of split key.");
        }
        return ret;
    }

    // warn: Types.NUMERIC is used for oracle! because oracle use NUMBER to
    // store INT, SMALLINT, INTEGER, and only oracle need to concern
    // Types.NUMERIC
    private static boolean isLongType(int type)
    {
        boolean isValidLongType = type == Types.BIGINT || type == Types.INTEGER || type == Types.SMALLINT || type == Types.TINYINT;

        if (SingleTableSplitUtil.dataBaseType == DataBaseType.Oracle) {
            isValidLongType |= type == Types.NUMERIC;
        }
        return isValidLongType;
    }

    private static boolean isStringType(int type)
    {
        return type == Types.CHAR || type == Types.NCHAR || type == Types.VARCHAR || type == Types.LONGVARCHAR || type == Types.NVARCHAR;
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

        String minMaxTemplate = "SELECT MIN(%s), MAX(%s) FROM %s";
        String pkRangeSQL = String.format(minMaxTemplate, splitPK, splitPK, table);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)", pkRangeSQL, where, splitPK);
        }
        return pkRangeSQL;
    }

    /**
     * support Number and String split
     *
     * @param splitPK primary key will be split
     * @param table table name
     * @param where where clause
     * @param configuration configuration
     * @param adviceNum the number of split
     * @return list of string
     */
    public static List<String> genSplitSqlForOracle(String splitPK, String table, String where, Configuration configuration, int adviceNum)
    {
        if (adviceNum < 1) {
            throw new IllegalArgumentException(String.format("The number of split should be greater than or equal 1, but it got %d.", adviceNum));
        }
        else if (adviceNum == 1) {
            return new ArrayList<>();
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
        String splitSql = String.format(sampleSqlTemplate, splitPK, table, percentage, whereSql, adviceNum, splitPK);

        int fetchSize = configuration.getInt(Key.FETCH_SIZE, 32);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        LOG.info("split pk [sql={}] is running... ", splitSql);
        ResultSet rs = null;
        List<Pair<Object, Integer>> splitRange = new ArrayList<>();
        try {
            rs = DBUtil.query(conn, splitSql, fetchSize);
            configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_MONTE_CARLO);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            while (DBUtil.asyncResultSetNext(rs)) {
                ImmutablePair<Object, Integer> eachPoint = new ImmutablePair<>(rs.getObject(1), rsMetaData.getColumnType(1));
                splitRange.add(eachPoint);
            }
        }
        catch (AddaxException e) {
            throw e;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to split table by split key.", e);
        }
        finally {
            DBUtil.closeDBResources(rs, null, null);
        }

        LOG.debug(JSON.toJSONString(splitRange));
        List<String> rangeSql = new ArrayList<>();
        int splitRangeSize = splitRange.size();
        // warn: splitRangeSize may be 0 or 1，切分规则为IS NULL以及 IS NOT NULL
        // demo: Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[24999930].
        if (splitRangeSize >= 2) {
            // warn: oracle Number is long type here
            if (isLongType(splitRange.get(0).getRight())) {
                BigInteger[] integerPoints = new BigInteger[splitRange.size()];
                for (int i = 0; i < splitRangeSize; i++) {
                    integerPoints[i] = new BigInteger(splitRange.get(i).getLeft().toString());
                }
                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(integerPoints, splitPK));
                // it's ok if splitRangeSize is 1
                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(integerPoints[0], integerPoints[splitRangeSize - 1], splitPK));
            }
            else if (isStringType(splitRange.get(0).getRight())) {
                // warn: treated as string type
                String[] stringPoints = new String[splitRange.size()];
                for (int i = 0; i < splitRangeSize; i++) {
                    stringPoints[i] = splitRange.get(i).getLeft().toString();
                }
                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(stringPoints, splitPK, "'", dataBaseType));
                // it's ok if splitRangeSize is 1
                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(stringPoints[0], stringPoints[splitRangeSize - 1],
                        splitPK, "'", dataBaseType));
            }
            else {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "the data type of split key is unsupported. it ONLY supports integer and string.");
            }
        }
        return rangeSql;
    }

    /**
     * common String split method
     *
     * @param configuration configuration
     * @param table the table which be queried
     * @param where where clause
     * @param minVal minimal value
     * @param maxVal maximal value
     * @param splitNum expected split number
     * @param pkName the column which split by
     * @return list of string
     */
    private static List<String> splitStringPk(Configuration configuration, String table, String where, String minVal, String maxVal,
            int splitNum, String pkName)
    {
        List<String> rangeList = new ArrayList<>();
        String splitSql;
        if (splitNum < 2) {
            rangeList.add(String.format("%s >= '%s' AND %s <= '%s'", pkName, minVal, pkName, maxVal));
            return rangeList;
        }
        if (StringUtils.isBlank(where)) {
            where = "1=1";
        }
        if (dataBaseType == DataBaseType.MySql) {
            splitSql = String.format("SELECT %1$s from (SELECT %1$s FROM %2$s WHERE %3$s ORDER BY RAND() LIMIT %4$d) T ORDER BY %1$s ASC",
                    pkName, table, where, splitNum - 1);
        }
        else if (dataBaseType == DataBaseType.PostgreSQL) {
            splitSql = String.format("SELECT %s FROM %s TABLESAMPLE SYSTEM(10) REPEATABLE(200) ORDER BY %s LIMIT %d",
                    pkName, table, pkName, splitNum - 1);
        }
        else {
            return RdbmsRangeSplitWrap.splitAndWrap(minVal, maxVal, splitNum, pkName, "'", dataBaseType);
        }
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        try (Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password)) {
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(splitSql);
            List<String> values = new ArrayList<>();
            while (resultSet.next()) {
                values.add(resultSet.getString(1));
            }
            String preVal = minVal;
            for (String val : values) {
                rangeList.add(String.format("%1$s >='%2$s' AND %1$s <'%3$s' ", pkName, preVal, val));
                preVal = val;
            }
            rangeList.add(String.format("%1$s >='%2$s' AND %1$s <='%3$s' ", pkName, preVal, maxVal));
            return rangeList;
        }
        catch (SQLException e) {
            LOG.error("Failed to split the table by split key[{}]", splitSql, e);
            return rangeList;
        }
    }
}
