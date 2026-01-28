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
import java.util.Set;
import java.util.StringJoiner;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

/**
 * Utility class for splitting a single table configuration into multiple task configurations.
 */
public class SingleTableSplitUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(SingleTableSplitUtil.class);

    private static final Set<JDBCType> LONG_TYPES = Set.of(
            JDBCType.BIGINT, JDBCType.INTEGER, JDBCType.SMALLINT, JDBCType.TINYINT
    );

    private static final Set<JDBCType> FLOAT_TYPES = Set.of(
            JDBCType.DECIMAL, JDBCType.NUMERIC,
            JDBCType.DOUBLE, JDBCType.FLOAT, JDBCType.REAL
    );

    private SingleTableSplitUtil()
    {
    }

    /**
     * Splits a single table configuration into multiple task configurations for parallel reading.
     * Uses primary key ranges to divide the table data across multiple reading tasks.
     *
     * @param dataBaseType The database type for generating appropriate SQL queries
     * @param configuration The configuration for the single table to split
     * @param adviceNum The number of parallel tasks to create
     * @return List of task configurations, each with specific query ranges
     */
    public static List<Configuration> splitSingleTable(DataBaseType dataBaseType, Configuration configuration, int adviceNum)
    {
        List<Configuration> pluginParams = new ArrayList<>();
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

        var rangeList = genPkRangeSQLForGeneric(dataBaseType, splitPkName, table, where, configuration, adviceNum);

        if (rangeList.isEmpty()) {
            //mean the split key only has null value
            LOG.warn("The min value is equal to the max value, or the split key has only null value to table {}. so we only have one slice.", table);
            configuration.set(Key.QUERY_SQL, buildQuerySql(column, table, where));
            pluginParams.add(configuration);
            return pluginParams;
        }

        StringJoiner allQuerySql = new StringJoiner("\n");

        for (String range : rangeList) {
            var tempConfig = configuration.clone();
            var tempQuerySql = buildQuerySql(column, table, where) + (hasWhere ? " AND " : " WHERE ") + range;

            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }

        if (configuration.getBool("pkExistsNull", false)) {
            var tempConfig = configuration.clone();
            var tempQuerySql = buildQuerySql(column, table, where)
                    + (hasWhere ? " AND " : " WHERE ")
                    + splitPkName + " IS NULL";
            allQuerySql.add(tempQuerySql);
            tempConfig.set(Key.QUERY_SQL, tempQuerySql);
            pluginParams.add(tempConfig);
        }
        LOG.info("After splitting for table {}, all query sql = [\n{}\n].", table, allQuerySql);

        return pluginParams;
    }

    /**
     * Builds a complete SQL query string with optional WHERE clause.
     *
     * @param column The column specification (e.g., "*" or "col1,col2")
     * @param table The table name to query
     * @param where The WHERE clause condition (can be null or empty)
     * @return Complete SQL query string
     */
    public static String buildQuerySql(String column, String table, String where)
    {
        return StringUtils.isBlank(where)
                ? String.format(Constant.QUERY_SQL_TEMPLATE_WITHOUT_WHERE, column, table)
                : String.format(Constant.QUERY_SQL_TEMPLATE, column, table, where);
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
        String splitPK = dataBaseType.quoteColumnName(configuration.getString(Key.SPLIT_PK).trim());
        String table = configuration.getString(Key.TABLE).trim();
        String where = configuration.getString(Key.WHERE, null);
        String pkRangeSQL = genPKSql(splitPK, table, where);

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        LOG.info("Get split pk min and max value sql [{}] is running...", pkRangeSQL);
        try (Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
                ResultSet rs = DBUtil.query(conn, pkRangeSQL, 1)) {

            MinMaxPackage minMaxPackage = new MinMaxPackage();
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

            checkForNullValues(configuration, conn, splitPK, table, where);

            return minMaxPackage;
        }
        catch (AddaxException e) {
            throw e;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to split the table.", e);
        }
    }

    private static void checkForNullValues(Configuration configuration, Connection conn, String splitPK, String table, String where)
    {
        String nullCheckSql = StringUtils.isBlank(where)
                ? "SELECT count(*) FROM " + table + " WHERE " + splitPK + " IS NULL"
                : "SELECT count(*) FROM " + table + " WHERE " + where + " AND " + splitPK + " IS NULL";

        try (ResultSet rs = DBUtil.query(conn, nullCheckSql, 1)) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("the split key has null value.");
                configuration.set("pkExistsNull", true);
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to split the table.", e);
        }
    }

    /**
     * Generates primary key range SQL queries for splitting table data across multiple tasks.
     * Supports numeric and string primary key types with appropriate splitting strategies.
     *
     * @param dataBaseType The database type for generating appropriate queries
     * @param splitPK The primary key column name to use for splitting
     * @param table The table name to split
     * @param where Optional WHERE clause to filter data
     * @param configuration Configuration containing connection details
     * @param adviceNum The number of splits to create
     * @return List of WHERE clause conditions for each split, or empty list if splitting not possible
     */
    public static List<String> genPkRangeSQLForGeneric(DataBaseType dataBaseType, String splitPK, String table, String where, Configuration configuration, int adviceNum)
    {
        if (adviceNum == 1) {
            return new ArrayList<>();
        }

        MinMaxPackage pkMinAndMaxValue = getPkMinAndMaxValue(dataBaseType, configuration);
        if (pkMinAndMaxValue.getMin() == null || pkMinAndMaxValue.getMax() == null || pkMinAndMaxValue.isSameValue()) {
            // mean the split key has only a null value, it cannot split
            return Collections.emptyList();
        }

        List<Object> rangeValue;
        if (pkMinAndMaxValue.isNumeric()) {
            LOG.info("The type of split key is numeric, so we use the math algorithm to split the table.");
            rangeValue = pkMinAndMaxValue.genSplitPoint(adviceNum);
            return genAllTypePkRangeWhereClause(splitPK, pkMinAndMaxValue, rangeValue);
        }

        String splitSql = genSplitPointSql(splitPK, table, where, adviceNum, dataBaseType, pkMinAndMaxValue);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);

        LOG.info("split pk [sql={}] is running... ", splitSql);

        rangeValue = new ArrayList<>();
        try (Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);
                ResultSet rs = DBUtil.query(conn, splitSql, adviceNum)) {

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

        LOG.debug(JSON.toJSONString(rangeValue));
        return genAllTypePkRangeWhereClause(splitPK, pkMinAndMaxValue, rangeValue);
    }

    /**
     * Generate SQL that get the split points, whose points is the boundary of the split
     * we can use math algorithm to get the split points, but it causes the data skew when the
     * split key is not uniform distributed. So we use the random algorithm to get the split points
     *
     * @param splitPK the split key
     * @param table the table name
     * @param whereSql the where clause
     * @param adviceNum the number of splits
     * @param dataBaseType the database type
     * @param minMaxPack {@link MinMaxPackage}
     * @return the SQL string that gets the split points
     */
    private static String genSplitPointSql(String splitPK, String table, String whereSql, int adviceNum, DataBaseType dataBaseType, MinMaxPackage minMaxPack)
    {
        if (minMaxPack.getType() == MinMaxPackage.PkType.STRING) {
            // For string type, we'll calculate the split points directly instead of using ORDER BY RAND()
            List<Object> splitPoints = calculateStringSplitPoints(
                    minMaxPack.getMin().toString(),
                    minMaxPack.getMax().toString(),
                    adviceNum - 1);

            // Store the calculated split points in configuration for use later
            return buildStringSplitPointQuery(splitPK, dataBaseType, splitPoints);
        }

        if (StringUtils.isBlank(whereSql)) {
            whereSql = " WHERE 1=1 ";
        }
        else {
            whereSql = "WHERE (" + whereSql + ") AND " + splitPK + " > " + minMaxPack.getMin() + " AND " + splitPK + "<" + minMaxPack.getMax();
        }

        return switch (dataBaseType) {
            case Oracle -> String.format("""
                            select %1$s from (
                                select %1$s from %2$s %3$s order by DBMS_RANDOM.VALUE
                            ) where rownum < %4$d order by %1$s""",
                    splitPK, table, whereSql, adviceNum);

            case SQLServer, Sybase -> String.format("""
                            select %1$s from (
                                select top %4$d %1$s from %2$s %3$s order by newid()
                            ) t order by %1$s""",
                    splitPK, table, whereSql, adviceNum - 1);

            case PostgreSQL, SQLite -> String.format("""
                            select %1$s from (
                                select %1$s from %2$s %3$s order by random() limit %4$d
                            ) t order by %1$s""",
                    splitPK, table, whereSql, adviceNum - 1);

            default -> String.format("""
                            select %1$s from (
                                select %1$s from %2$s %3$s order by rand() limit %4$d
                            ) t order by %1$s""",
                    splitPK, table, whereSql, adviceNum - 1);
        };
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
        boolean isString = minMaxPackage.getType() == MinMaxPackage.PkType.STRING;

        String singleSqlTemplate = isString ? "%s >= '%s' AND %s <= '%s'" : "%s >= %s AND %s <= %s";
        String middleSqlTemplate = isString ? "%s >= '%s' AND %s < '%s'" : "%s >= %s AND %s < %s";
        String lastSqlTemplate = isString ? "%s >= '%s' AND %s <= '%s'" : "%s >= %s AND %s <= %s";

        Object min = minMaxPackage.getMin();
        Object max = minMaxPackage.getMax();
        if (rangeValues.isEmpty()) {
            rangeSql.add(String.format(singleSqlTemplate, pkName, min, pkName, max));
            return rangeSql;
        }

        var allPoints = new ArrayList<>(rangeValues.size() + 1);
        allPoints.add(min);
        allPoints.addAll(rangeValues);

        for (int i = 0; i < allPoints.size() - 1; i++) {
            rangeSql.add(String.format(middleSqlTemplate, pkName, allPoints.get(i), pkName, allPoints.get(i + 1)));
        }

        rangeSql.add(String.format(lastSqlTemplate, pkName, allPoints.get(allPoints.size() - 1), pkName, max));

        return rangeSql;
    }

    private static boolean isLongType(int type)
    {
        return LONG_TYPES.contains(JDBCType.valueOf(type));
    }

    private static boolean isFloatType(int type)
    {
        return FLOAT_TYPES.contains(JDBCType.valueOf(type));
    }

    /**
     * Generates SQL query to get the minimum and maximum values of the primary key column.
     *
     * @param splitPK The primary key column name (should be properly quoted)
     * @param table The table name to query
     * @param where Optional WHERE clause to filter the range calculation
     * @return SQL query string to retrieve MIN and MAX values of the primary key
     */
    public static String genPKSql(String splitPK, String table, String where)
    {
        String pkRangeSQL = String.format("SELECT MIN(%s), MAX(%s) FROM %s", splitPK, splitPK, table);

        if (StringUtils.isNotBlank(where)) {
            return String.format("%s WHERE (%s AND %s IS NOT NULL)", pkRangeSQL, where, splitPK);
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
        // Generate split points using Stream API
        if (numSplits <= 0 || min.compareTo(max) >= 0) {
            return new ArrayList<>();
        }

        return java.util.stream.IntStream.rangeClosed(1, numSplits)
                .mapToObj(i -> calculateStringBetween(min, max, (double) i / (numSplits + 1)))
                .collect(java.util.stream.Collectors.toList());
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

        // Get common prefix length using Stream
        int minLength = Math.min(minChars.length, maxChars.length);
        int commonPrefixLength = 0;

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
            return new String(minChars, 0, targetLength);
        }

        // Create a result array based on minChars
        char[] result = Arrays.copyOf(minChars, minChars.length);
        // Calculate a candidate character in the area where min and max differ
        char candidate = (char) (minChars[commonPrefixLength]
                + (int) ((maxChars[commonPrefixLength] - minChars[commonPrefixLength]) * fraction));

        // Clamp the candidate to the allowed range
        candidate = clampToAllowedChar(candidate);
        result[commonPrefixLength] = candidate;

        // Truncate the remaining part
        return new String(result, 0, commonPrefixLength + 1);
    }

    // Clamp the character to the nearest valid character from ALLOWED_CHARS
    private static char clampToAllowedChar(char c)
    {
        // Define character set using predefined string
        final String ALLOWED_CHARS_STR = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";
        final char[] ALLOWED_CHARS = ALLOWED_CHARS_STR.toCharArray();

        if (c <= ALLOWED_CHARS[0]) {
            return ALLOWED_CHARS[0];
        }
        if (c >= ALLOWED_CHARS[ALLOWED_CHARS.length - 1]) {
            return ALLOWED_CHARS[ALLOWED_CHARS.length - 1];
        }

        // Find the nearest character using linear search
        for (int i = 0; i < ALLOWED_CHARS.length - 1; i++) {
            if (c >= ALLOWED_CHARS[i] && c <= ALLOWED_CHARS[i + 1]) {
                // Choose the closer one
                return (c - ALLOWED_CHARS[i]) <= (ALLOWED_CHARS[i + 1] - c) ?
                        ALLOWED_CHARS[i] : ALLOWED_CHARS[i + 1];
            }
        }
        return c;
    }

    /**
     * Build a query that returns the pre-calculated split points
     */
    private static String buildStringSplitPointQuery(String splitPK, DataBaseType databaseType, List<Object> splitPoints)
    {
        if (splitPoints.isEmpty()) {
            return "";
        }

        StringJoiner unionQuery = new StringJoiner(" UNION ALL ");
        if (databaseType == DataBaseType.Oracle) {
            splitPoints.forEach(point ->
                    unionQuery.add(String.format("SELECT '%s' AS %s FROM DUAL", point, splitPK)));
        } else {
            splitPoints.forEach(point ->
                    unionQuery.add(String.format("SELECT '%s' AS %s", point, splitPK)));
        }
        return String.format("SELECT * FROM (%s) t ORDER BY %s", unionQuery, splitPK);
    }
}
