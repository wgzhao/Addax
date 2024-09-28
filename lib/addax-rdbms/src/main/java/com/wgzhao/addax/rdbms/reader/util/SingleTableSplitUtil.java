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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

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
        if (adviceNum < 1) {
            throw new IllegalArgumentException("The number of split should be greater than or equal 1, but it got " + adviceNum);
        }
        rangeList = genPkRangeSQLForGeneric(splitPkName, table, where, configuration, adviceNum);
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

        Configuration tempConfig = configuration.clone();
        tempQuerySql = buildQuerySql(column, table, where)
                + (hasWhere ? " AND " : " WHERE ")
                + splitPkName + " IS NULL";
        allQuerySql.add(tempQuerySql);
        tempConfig.set(Key.QUERY_SQL, tempQuerySql);
        pluginParams.add(tempConfig);

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
     * support Number and String split
     *
     * @param splitPK primary key will be split
     * @param table table name
     * @param where where clause
     * @param configuration configuration
     * @param adviceNum the number of split
     * @return {@link List} of where clause
     */
    public static List<String> genPkRangeSQLForGeneric(String splitPK, String table, String where, Configuration configuration, int adviceNum)
    {
        if (adviceNum == 1) {
            return new ArrayList<>();
        }

        List<Object> rangeValue = new ArrayList<>();
        MinMaxPackage pkMinAndMaxValue = getPkMinAndMaxValue(configuration);
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
        if (minMaxPack.getType() == Constant.PK_TYPE_STRING) {
            whereSql = whereSql + " AND " + splitPK + " > '" + minMaxPack.getMin().toString() + "' AND " + splitPK + "< '" + minMaxPack.getMax().toString() + "'";
        }
        else {
            whereSql = whereSql + " AND " + splitPK + " > " + minMaxPack.getMin() + " AND " + splitPK + "<" + minMaxPack.getMax();
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
     * pk >= min and pk < splitPoint1
     * pk >= splitPoint1 and pk < splitPoint2
     * ....
     * pk >= splitPointN and pk <= max
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

        boolean isString = minMaxPackage.getType() == Constant.PK_TYPE_STRING;
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
}
