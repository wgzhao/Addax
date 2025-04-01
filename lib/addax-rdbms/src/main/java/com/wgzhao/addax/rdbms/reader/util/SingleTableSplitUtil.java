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
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

public class SingleTableSplitUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(SingleTableSplitUtil.class);

    private SingleTableSplitUtil()
    {
    }

    public static List<Configuration> splitSingleTable(DataBaseType dataBaseType, Configuration configuration, int adviceNum)
    {
        List<Configuration> pluginParams = new ArrayList<>();
        List<String> rangeList;
        String splitPkName = configuration.getString(Key.SPLIT_PK);
        String column = configuration.getString(Key.COLUMN);
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);
        boolean hasWhere = StringUtils.isNotBlank(where);
        if (adviceNum < 1) {
            throw new IllegalArgumentException("The number of split should be greater than or equal 1, but it got " + adviceNum);
        }
        if (adviceNum == 1) {
            LOG.warn("The adviceNumber is 1, so we only have one slice.");
            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
            pluginParams.add(configuration);
            return pluginParams;
        }

        rangeList = genPkRangeSQLForGeneric(dataBaseType, splitPkName, table, where, configuration, adviceNum);

        if (rangeList.isEmpty()) {
            //mean the split key only has null value
            LOG.warn("The min value is equal to the max value, or the split key has only null value to table {}. so we only have one slice.", table);
            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
            pluginParams.add(configuration);
            return pluginParams;
        }

        String tempQuerySql;
        StringJoiner allQuerySql = new StringJoiner("\n");

        for (String range : rangeList) {
            Configuration tempConfig = configuration.clone();

            tempQuerySql = buildQuerySql(column, table, where) + (hasWhere ? " AND " : " WHERE ") + range;

            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }

        if (configuration.getBool("pkExistsNull", false)) {
            Configuration tempConfig = configuration.clone();
            tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere ? " AND " : " WHERE ")
                    + splitPkName + " IS NULL";
            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }
        LOG.info("After splitting for table {}, all query sql = [\n{}\n].", table, allQuerySql);

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
     * @param dataBaseType {@link DataBaseType}
     * @param configuration {@link Configuration}
     * @return {@link MinMaxPackage}
     * e.g {"min": 1, "max": 100, "type": "long"} `type` is one of `long`, `float`, `string`
     */
    private static MinMaxPackage getPkMinAndMaxValue(DataBaseType dataBaseType, Configuration configuration)
    {
        String splitPK = configuration.getString(Key.SPLIT_PK).trim();
        String table = configuration.getString(Key.TABLE).trim();
        String where = configuration.getString(Key.WHERE, null);
        String pkRangeSQL = genPKSql(splitPK, table, where);

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        MinMaxPackage minMaxPackage = new MinMaxPackage();
        ResultSet rs = null;
        try {
            rs = DBUtil.query(conn, pkRangeSQL, 1);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isLongType(rsMetaData.getColumnType(1))) {
                minMaxPackage.setType(MinMaxPackage.PkType.LONG);
            }
            else if (isFloatType(rsMetaData.getColumnType(1))) {
                minMaxPackage.setType(MinMaxPackage.PkType.FLOAT);
            }
            else {
                minMaxPackage.setType(MinMaxPackage.PkType.STRING);
            }
            while (DBUtil.asyncResultSetNext(rs)) {
                minMaxPackage.setMin(rs.getObject(1));
                minMaxPackage.setMax(rs.getObject(2));
            }
            rs.close();
            // the pk exists null at the current condition
            if (StringUtils.isBlank(where)) {
                rs = DBUtil.query(conn, "SELECT count(*) FROM " + table + " WHERE " + splitPK + " IS NULL", 1);
            }
            else {
                rs = DBUtil.query(conn, "SELECT count(*) FROM " + table + " WHERE " + where + " AND " + splitPK + " IS NULL", 1);
            }
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("the split key has null value.");
                //minMaxPackage.setPkExistsNull(true);
                configuration.set("pkExistsNull", true);
            }
            rs.close();
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
        return minMaxPackage;
    }

    /**
     * support Number and String split
     *
     * @param splitPK primary key will be split
     * @param table table name
     * @param where where clause
     * @param configuration configuration
     * @param adviceNum the number of split
     * @return 1. empty list of the min value is equal to max value, or the split key has only null value;
     * 2. {@link List} of where clause
     */
    public static List<String> genPkRangeSQLForGeneric(DataBaseType dataBaseType, String splitPK, String table, String where, Configuration configuration, int adviceNum)
    {
        if (adviceNum == 1) {
            return new ArrayList<>();
        }

        List<Object> rangeValue = new ArrayList<>();
        MinMaxPackage pkMinAndMaxValue = getPkMinAndMaxValue(dataBaseType, configuration);
        if (pkMinAndMaxValue.getMin() == null || pkMinAndMaxValue.getMax() == null || pkMinAndMaxValue.isSameValue()) {
            // mean the split key has only null value, it can not split
            return Collections.emptyList();
        }

        if (pkMinAndMaxValue.isNumeric()) {
            LOG.info("The type of split key is numeric, so we use the math algorithm to split the table.");
            rangeValue = pkMinAndMaxValue.genSplitPoint(adviceNum);
            return genAllTypePkRangeWhereClause(splitPK, pkMinAndMaxValue, rangeValue);
        }

        String splitSql = genSplitPointSql(splitPK, table, where, adviceNum, dataBaseType, pkMinAndMaxValue);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
        LOG.info("split pk [sql={}] is running... ", splitSql);
        ResultSet rs = null;

        try {
            rs = DBUtil.query(conn, splitSql, adviceNum);
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
        return genAllTypePkRangeWhereClause(splitPK, pkMinAndMaxValue, rangeValue);
    }

    /**
     * generate sql that get the split points, whose points is the boundary of the split
     * we can use math algorithm to get the split points, but it causes the data skew when the
     * split key is not uniform distributed. So we use the random algorithm to get the split points
     *
     * @param splitPK the split key
     * @param table the table name
     * @param whereSql the where clause
     * @param adviceNum the number of split
     * @param dataBaseType the database type
     * @param minMaxPack {@link MinMaxPackage}
     * @return the sql string that get the split points
     */
    private static String genSplitPointSql(String splitPK, String table, String whereSql, int adviceNum, DataBaseType dataBaseType, MinMaxPackage minMaxPack)
    {
        String sql;
        if (StringUtils.isBlank(whereSql)) {
            whereSql = " WHERE 1=1 ";
        }
        // If it's a string type and we need to split efficiently
        if (minMaxPack.getType() == MinMaxPackage.PkType.STRING) {
            // For string type, we'll calculate the split points directly instead of using ORDER BY RAND()
            List<Object> splitPoints = calculateStringSplitPoints(
                    minMaxPack.getMin().toString(),
                    minMaxPack.getMax().toString(),
                    adviceNum - 1);

            // Store the calculated split points in configuration for use later
            return buildStringSplitPointQuery(splitPK, table, whereSql, splitPoints);
        }
        else {
            whereSql = "WHERE (" + whereSql + ") AND " + splitPK + " > " + minMaxPack.getMin() + " AND " + splitPK + "<" + minMaxPack.getMax();
        }

        if (dataBaseType == DataBaseType.Oracle) {
            sql = String.format("select %1$s from (select %1$s from %2$s %3$s order by DBMS_RANDOM.VALUE) where rownum < %4$d order by %1$s",
                    splitPK, table, whereSql, adviceNum);
        }
        else if (dataBaseType == DataBaseType.SQLServer || dataBaseType == DataBaseType.Sybase) {
            sql = String.format("select %1$s from (select top %4$d %1$s from %2$s %3$s order by newid()) t order by %1$s",
                    splitPK, table, whereSql, adviceNum - 1);
        }
        else if (dataBaseType == DataBaseType.PostgreSQL || dataBaseType == DataBaseType.SQLite) {
            sql = String.format("select %1s from (select %1$s from %2$s %3$s order by random() limit %4$d) t order by %1$s",
                    splitPK, table, whereSql, adviceNum - 1);
        }
        else {
            // include mysql, mariadb, clickhouse, hive, trinodb, presto, doris, phoenix with hbase
            sql = String.format("select %1$s from (select %1$s from %2$s %3$s order by rand() limit %4$d) t order by %1$s",
                    splitPK, table, whereSql, adviceNum - 1);
        }
        return sql;
    }

    /**
     * generate all split query where clause like the following:
     * <p>
     * pk &ge; min and pk &lt; splitPoint1
     * pk &ge; splitPoint1 and pk &lt; splitPoint2
     * ....
     * pk &ge; splitPointN and pk &le; max
     * </p>
     *
     * @param pkName the split key name
     * @param minMaxPackage {@link MinMaxPackage}
     * @param rangeValues the list of split points
     * @return the list of where clause
     */
    public static List<String> genAllTypePkRangeWhereClause(String pkName, MinMaxPackage minMaxPackage, List<Object> rangeValues)
    {
        List<String> rangeSql = new ArrayList<>();
        String singleSqlTemplate;
        String middleSqlTemplate;
        String lastSqlTemplate;

        boolean isString = minMaxPackage.getType() == MinMaxPackage.PkType.STRING;
        if (isString) {
            singleSqlTemplate = "%s >= '%s' AND %s <= '%s'";
            middleSqlTemplate = "%s >= '%s' AND %s < '%s'";
            lastSqlTemplate = "%s >= '%s' AND %s <= '%s'";
        }
        else {
            singleSqlTemplate = "%s >= %s AND %s <= %s";
            middleSqlTemplate = "%s >= %s AND %s < %s";
            lastSqlTemplate = "%s >= %s AND %s <= %s";
        }
        Object min = minMaxPackage.getMin();
        Object max = minMaxPackage.getMax();
        if (rangeValues.isEmpty()) {
            rangeSql.add(String.format(singleSqlTemplate, pkName, min, pkName, max));
            return rangeSql;
        }
        rangeValues.add(0, min);
        for (int i = 0; i < rangeValues.size() - 1; i++) {
            rangeSql.add(String.format(middleSqlTemplate, pkName, rangeValues.get(i), pkName, rangeValues.get(i + 1)));
        }

        rangeSql.add(String.format(lastSqlTemplate, pkName, rangeValues.get(rangeValues.size() - 1), pkName, max));

        return rangeSql;
    }

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
     * Calculate split points for string type primary keys
     *
     * @param min Minimum value of the primary key
     * @param max Maximum value of the primary key
     * @param numSplits Number of split points to generate
     * @return List of split points
     */
    private static List<Object> calculateStringSplitPoints(String min, String max, int numSplits)
    {
        List<Object> splitPoints = new ArrayList<>();
        if (numSplits <= 0 || min.compareTo(max) >= 0) {
            return splitPoints;
        }

        // Use lexicographic distribution to create split points
        for (int i = 1; i <= numSplits; i++) {
            // Create a weighted average of min and max strings
            String splitPoint = calculateStringBetween(min, max, (double) i / (numSplits + 1));
            splitPoints.add(splitPoint);
        }

        return splitPoints;
    }

    /**
     * Calculate a string that is approximately at the given fraction between min and max
     *
     * @param min Minimum string value
     * @param max Maximum string value
     * @param fraction Fraction between 0 and 1
     * @return A string value at the approximate position
     */
    private static String calculateStringBetween(String min, String max, double fraction)
    {
        // Convert to character arrays for easier manipulation
        char[] minChars = min.toCharArray();
        char[] maxChars = max.toCharArray();

        // Get common prefix length
        int commonPrefixLength = 0;
        int minLength = Math.min(minChars.length, maxChars.length);

        while (commonPrefixLength < minLength && minChars[commonPrefixLength] == maxChars[commonPrefixLength]) {
            commonPrefixLength++;
        }

        // If they share the entire prefix, return a value based on length
        if (commonPrefixLength == minLength) {
            if (minChars.length == maxChars.length) {
                return min; // They're equal, can't create a midpoint
            }

            // Return a value with length between min and max
            int targetLength = (int) (minChars.length + fraction * (maxChars.length - minChars.length));
            char[] result = Arrays.copyOf(minChars, targetLength);
            return new String(result);
        }

        // Create a new string with the common prefix and a character between the differing positions
        char[] result = Arrays.copyOf(minChars, minChars.length);
        result[commonPrefixLength] = (char) (minChars[commonPrefixLength] +
                (int) (fraction * (maxChars[commonPrefixLength] - minChars[commonPrefixLength])));

        // If we added to the character, truncate the rest
        if (result[commonPrefixLength] > minChars[commonPrefixLength]) {
            return new String(Arrays.copyOf(result, commonPrefixLength + 1));
        }

        return new String(result);
    }

    /**
     * Build a query that returns the pre-calculated split points
     */
    private static String buildStringSplitPointQuery(String splitPK, String table, String whereSql, List<Object> splitPoints)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM (");

        for (int i = 0; i < splitPoints.size(); i++) {
            if (i > 0) {
                sb.append(" UNION ALL ");
            }
            sb.append("SELECT '").append(splitPoints.get(i)).append("' AS ").append(splitPK);
        }

        sb.append(") t ORDER BY ").append(splitPK);
        return sb.toString();
    }
}
