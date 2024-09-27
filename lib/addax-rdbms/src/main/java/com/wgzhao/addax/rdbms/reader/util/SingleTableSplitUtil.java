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
import com.wgzhao.addax.rdbms.util.RdbmsRangeSplitWrap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

public class SingleTableSplitUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(SingleTableSplitUtil.class);

    public static DataBaseType dataBaseType;

    public static class MinMaxPackage
    {
        private Object min;
        private Object max;
        private Object type;

        public MinMaxPackage()
        {
            this.min = null;
            this.max = null;
            this.type = null;
        }

        public Object getMin()
        {
            return min;
        }

        public void setMin(Object min)
        {
            this.min = min;
        }

        public Object getMax()
        {
            return max;
        }

        public void setMax(Object max)
        {
            this.max = max;
        }

        public Object getType()
        {
            return type;
        }

        public void setType(Object type)
        {
            this.type = type;
        }

        public boolean isLong()
        {
            return type == Constant.PK_TYPE_LONG;
        }

        public boolean isFloat()
        {
            return type == Constant.PK_TYPE_FLOAT;
        }

        public boolean isString()
        {
            return type == Constant.PK_TYPE_STRING;
        }
    }

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
        MinMaxPackage minMaxPK = getPkMinAndMaxValue(configuration);
        rangeList = genSplitSql(splitPkName, table, where, adviceNum, dataBaseType, minMaxPK);
        
//        if (dataBaseType == DataBaseType.Oracle) {
//            rangeList = genPkRangeSQLForOracle(splitPkName, table, where, configuration, adviceNum);
//            // warn: mysql etc to be added...
//        }
//        else {
//
//            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
//            if (null == minMaxPK.getMin() || null == minMaxPK.getMax()) {
//                // 切分后获取到的start/end 有 Null 的情况
//                pluginParams.add(configuration);
//                return pluginParams;
//            }
//
//            boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration.getString(Constant.PK_TYPE));
//            boolean isFloatType = Constant.PK_TYPE_FLOAT.equals(configuration.getString(Constant.PK_TYPE));
//
//            if (minMaxPK.isLong()) {
//                rangeList = genPkRangeSQL((long) minMaxPK.getMin(), (long) minMaxPK.getMax(), adviceNum, splitPkName);
//            }
//            else if (isFloatType) {
//                rangeList = genPkRangeSQL(new BigDecimal(minMaxPK.getMin().toString()), new BigDecimal(minMaxPK.getMax().toString()), adviceNum, splitPkName);
//            }
//            else {
//                rangeList = genPkRangeSQLByQuery(configuration, table, where, minMaxPK.getMin().toString(), minMaxPK.getMax().toString(), adviceNum, splitPkName);
////                rangeList = genPkRange(minMaxPK.getLeft().toString(), minMaxPK.getRight().toString(), adviceNum, splitPkName);
//            }
//        }
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
        if (where == null || !where.contains(splitPkName)) {
            Configuration tempConfig = configuration.clone();
            tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere ? " AND " : " WHERE ")
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

    /**
     * get the min and max value of the split key
     *
     * @param configuration {@link Configuration}
     * @return {@link MinMaxPackage}
     * e.g {"min": 1, "max": 100, "type": "long"} `type` is one of `long`, `float`, `string`
     */
    private static MinMaxPackage getPkMinAndMaxValue(Configuration configuration)
    {
        String splitPK = configuration.getString(Key.SPLIT_PK).trim();
        String table = configuration.getString(Key.TABLE).trim();
        String where = configuration.getString(Key.WHERE, null);
        String pkRangeSQL = genPKSql(splitPK, table, where);

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        MinMaxPackage result = new MinMaxPackage();
        ResultSet rs = null;
        try {
            rs = DBUtil.query(conn, pkRangeSQL, 1);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isLongType(rsMetaData.getColumnType(1))) {
                result.setType(Constant.PK_TYPE_LONG);
            }
            else if (isFloatType(rsMetaData.getColumnType(1))) {
                result.setType(Constant.PK_TYPE_FLOAT);
            }
            else {
                result.setType(Constant.PK_TYPE_STRING);
            }
            while (DBUtil.asyncResultSetNext(rs)) {
                result.setMin(rs.getObject(1));
                result.setMax(rs.getObject(2));
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
            DBUtil.closeDBResources(null, conn);
        }
        return result;
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
        List<JDBCType> longTypeList = Arrays.asList(
                JDBCType.BIGINT, JDBCType.INTEGER, JDBCType.SMALLINT, JDBCType.TINYINT
        );
        return longTypeList.contains(JDBCType.valueOf(type));
    }

    private static boolean isFloatType(int type)
    {
        List<JDBCType> floatTypeList = Arrays.asList(JDBCType.DECIMAL, JDBCType.NUMERIC,
                JDBCType.DOUBLE, JDBCType.FLOAT, JDBCType.REAL
        );
        return floatTypeList.contains(JDBCType.valueOf(type));
    }

    private static boolean isStringType(int type)
    {
        return type == Types.CHAR || type == Types.NCHAR || type == Types.VARCHAR || type == Types.LONGVARCHAR || type == Types.NVARCHAR;
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
    public static List<String> genPkRangeSQLForGeneric(String splitPK, String table, String where, Configuration configuration, int adviceNum)
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
        MinMaxPackage pkMinAndMaxValue = getPkMinAndMaxValue(configuration);
        String splitSql = genSplitSql(splitPK, table, whereSql, adviceNum, dataBaseType, pkMinAndMaxValue);

        int fetchSize = configuration.getInt(Key.FETCH_SIZE, 32);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        LOG.info("split pk [sql={}] is running... ", splitSql);
        ResultSet rs = null;
        List<Object> rangeValue = new ArrayList<>();
        try {
            rs = DBUtil.query(conn, splitSql, fetchSize);
            while (DBUtil.asyncResultSetNext(rs)) {
                rangeValue.add(rs.getObject(1));
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

        LOG.debug(JSON.toJSONString(rangeValue));
        List<String> rangeSql = genPkRangeSQL(splitPK, pkMinAndMaxValue, rangeValue);
//        int splitRangeSize = rangeValue.size();
        // warn: splitRangeSize may be 0 or 1，切分规则为IS NULL以及 IS NOT NULL
        // demo: Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[24999930].
//        if (splitRangeSize >= 2) {
//            // warn: oracle Number is long type here
//            if (isLongType(splitRange.get(0).getRight())) {
//                BigInteger[] integerPoints = new BigInteger[splitRange.size()];
//                for (int i = 0; i < splitRangeSize; i++) {
//                    integerPoints[i] = new BigInteger(splitRange.get(i).getLeft().toString());
//                }
//                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(integerPoints, splitPK));
//                // it's ok if splitRangeSize is 1
//                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(integerPoints[0], integerPoints[splitRangeSize - 1], splitPK));
//            }
//            else if (isStringType(splitRange.get(0).getRight())) {
//                // warn: treated as string type
//                String[] stringPoints = new String[splitRange.size()];
//                for (int i = 0; i < splitRangeSize; i++) {
//                    stringPoints[i] = splitRange.get(i).getLeft().toString();
//                }
//                rangeSql.addAll(RdbmsRangeSplitWrap.wrapRange(stringPoints, splitPK, "'", dataBaseType));
//                // it's ok if splitRangeSize is 1
//                rangeSql.add(RdbmsRangeSplitWrap.wrapFirstLastPoint(stringPoints[0], stringPoints[splitRangeSize - 1],
//                        splitPK, "'", dataBaseType));
//            }
//            else {
//                throw AddaxException.asAddaxException(CONFIG_ERROR,
//                        "the data type of split key is unsupported. it ONLY supports integer and string.");
//            }
//        }
        return rangeSql;
    }

    private static String genSplitSql(String splitPK, String table, String whereSql, int adviceNum, DataBaseType dataBaseType, MinMaxPackage minMaxPack)
    {
        String sql = null;
        if (StringUtils.isBlank(whereSql)) {
            whereSql = " WHERE 1=1 ";
        }
        if (minMaxPack.getType() == Constant.PK_TYPE_STRING) {
            whereSql = whereSql + " AND " + splitPK + " > '" + minMaxPack.getMin().toString() + "' AND " + splitPK + "< '" + minMaxPack.getMax().toString() + "'";
        }
        else {
            whereSql = whereSql + " AND " + splitPK + " > " + minMaxPack.getMin() + " AND " + splitPK + "<" + minMaxPack.getMax();
        }
        if (dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.ClickHouse) {
            sql = String.format("select %1$s from (select %1$s from %2$s %3$s order by rand() limit %4$d) t order by %1$s",
                    splitPK, table, whereSql, adviceNum - 1);
        }
        else if (dataBaseType == DataBaseType.Oracle) {
            sql = String.format("select %1$s from (select %1$s from %2$s %3$s order by DBMS_RANDOM.VALUE) where rownum < %4$d order by %1$s",
                    splitPK, table, whereSql, adviceNum);
        }
        else if (dataBaseType == DataBaseType.SQLServer) {
            sql = String.format("select %1$s from (select top %4$d %1$s from %2$s %3$s order by newid()) t order by %1$s",
                    splitPK, table, whereSql, adviceNum - 1);
        }
        else if (dataBaseType == DataBaseType.PostgreSQL) {
            sql = String.format("select %1s from (select %1$s from %2$s %3$s order by random() limit %4$d) t order by %1$s",
                    splitPK, table, whereSql, adviceNum - 1);
        }
        return sql;
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
    public static List<String> genPkRangeSQLForOracle(String splitPK, String table, String where, Configuration configuration, int adviceNum)
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
     * get the split range sql by query database
     * it should be invoke when the split key is string type
     *
     * @param configuration configuration
     * @param table the table which be queried
     * @param where where clause
     * @param minVal minimal value
     * @param maxVal maximal value
     * @param splitNum expected split number
     * @param pkName the column which split by
     * @return {@link List} of query string
     */
    private static List<String> genPkRangeSQLByQuery(Configuration configuration, String table, String where, String minVal, String maxVal,
            int splitNum, String pkName)
    {
        List<String> rangeList = new ArrayList<>();
        String splitSql;
        boolean isLong = configuration.getString(Constant.PK_TYPE).equals(Constant.PK_TYPE_LONG);
        if (splitNum < 2) {
            rangeList.add(String.format("%s >= '%s' AND %s <= '%s'", pkName, minVal, pkName, maxVal));
            return rangeList;
        }
        if (StringUtils.isBlank(where)) {
            where = "where 1=1 ";
        }
        MinMaxPackage pkMinAndMaxValue = getPkMinAndMaxValue(configuration);
        splitSql = genSplitSql(pkName, table, where, splitNum, dataBaseType, pkMinAndMaxValue);
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

    /**
     * common long split method
     *
     * @param minVal minimal value
     * @param maxVal maximal value
     * @param splitNum expected split number
     * @param pkName the column which split by
     * @return list of split query sql, do not include is null clause
     */
    public static List<String> genPkRangeSQL(long minVal, long maxVal, int splitNum, String pkName)
    {
        List<Number> rangeList = new ArrayList<>();
        List<String> rangeSql = new ArrayList<>();
        if (splitNum < 2) {
            rangeSql.add(String.format("%s >= %s AND %s <= %s", pkName, minVal, pkName, maxVal));
            return rangeSql;
        }

        long step = (maxVal - minVal) / splitNum;
        for (int i = 0; i < splitNum; i++) {
            rangeList.add(minVal + i * step);
        }

        for (int i = 0; i < rangeList.size() - 1; i++) {
            rangeSql.add(String.format("%s >= %s AND %s < %s", pkName, rangeList.get(i), pkName, rangeList.get(i + 1)));
        }
        rangeSql.add(String.format("%s >= %s AND %s <= %s", pkName, rangeList.get(rangeList.size() - 1), pkName, maxVal));
        return rangeSql;
    }

    public static List<String> genPkRangeSQL(Number minVal, Number maxVal, int splitNum, String pkName)
    {
        List<Number> rangeList = new ArrayList<>();
        List<String> rangeSql = new ArrayList<>();
        if (splitNum < 2) {
            rangeSql.add(String.format("%s >= %s AND %s <= %s", pkName, minVal, pkName, maxVal));
            return rangeSql;
        }

        long step = Math.round((maxVal.doubleValue() - minVal.doubleValue()) / splitNum);
        for (int i = 0; i < splitNum; i++) {
            rangeList.add(minVal.doubleValue() + i * step);
        }

        for (int i = 0; i < rangeList.size() - 1; i++) {
            rangeSql.add(String.format("%s >= %s AND %s < %s", pkName, rangeList.get(i), pkName, rangeList.get(i + 1)));
        }
        rangeSql.add(String.format("%s >= %s AND %s <= %s", pkName, rangeList.get(rangeList.size() - 1), pkName, maxVal));
        return rangeSql;
    }

    public static List<String> genPkRangeSQL(String minVal, String maxVal, int splitNum, String pkName)
    {
        List<String> rangeList = new ArrayList<>();
        List<String> rangeSql = new ArrayList<>();
        if (splitNum < 2) {
            rangeSql.add(String.format("%s >= '%s' AND %s <= '%s'", pkName, minVal, pkName, maxVal));
            return rangeSql;
        }

        int minLen = minVal.length();
        int maxLen = maxVal.length();
        int step = (maxLen - minLen) / splitNum;
        for (int i = 0; i < splitNum; i++) {
            rangeList.add(minVal + StringUtils.repeat("\u0000", step * i));
        }

        for (int i = 0; i < rangeList.size() - 1; i++) {
            rangeSql.add(String.format("%s >= '%s' AND %s < '%s'", pkName, rangeList.get(i), pkName, rangeList.get(i + 1)));
        }
        rangeSql.add(String.format("%s >= '%s' AND %s <= '%s'", pkName, rangeList.get(rangeList.size() - 1), pkName, maxVal));
        return rangeSql;
    }

    public static List<String> genPkRangeSQL(String pkName, MinMaxPackage minMaxPackage, List<Object> rangeValues)
    {
        List<String> rangeList = new ArrayList<>();
        List<String> rangeSql = new ArrayList<>();
        boolean isString = minMaxPackage.getType() == Constant.PK_TYPE_STRING;
        Object min = minMaxPackage.getMin();
        Object max = minMaxPackage.getMax();
        if (rangeValues.isEmpty()) {
            if (isString) {
                rangeSql.add(String.format("%s >= '%s' AND %s <= '%s'", pkName, min, pkName, max));
            }

            else {
                rangeSql.add(String.format("%s >= %s AND %s <= %s", pkName, min, pkName, max));
            }
            return rangeSql;
        }
        rangeValues.add(0, min);
        for ( int i = 0; i < rangeValues.size() - 1; i++) {
            if (isString) {
                rangeSql.add(String.format("%s >= '%s' AND %s < '%s'", pkName, rangeValues.get(i), pkName, rangeValues.get(i + 1)));
            } else {
                rangeSql.add(String.format("%s >= %s AND %s < %s", pkName, rangeValues.get(i), pkName, rangeValues.get(i + 1)));
            }
        }
        if (isString) {
            rangeSql.add(String.format("%s >= '%s' AND %s <= '%s'", pkName, rangeValues.get(rangeValues.size() - 1), pkName, max));
        } else {
            rangeSql.add(String.format("%s >= %s AND %s <= %s", pkName, rangeValues.get(rangeValues.size() - 1), pkName, max));
        }
        return rangeSql;
    }
}
