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

package com.wgzhao.addax.plugin.reader.hbase20xsqlreader;

import com.wgzhao.addax.common.base.HBaseConstant;
import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.util.RdbmsRangeSplitWrap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.exception.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.exception.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.exception.ErrorCode.REQUIRED_VALUE;

public class HBase20SQLReaderHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HBase20SQLReaderHelper.class);

    private final Configuration configuration;

    private Connection connection;
    private List<String> querySql;
    private String fullTableName;
    private List<String> columnNames;
    private String splitKey;
    private List<Object> splitPoints;

    public HBase20SQLReaderHelper(Configuration configuration)
    {
        this.configuration = configuration;
    }

    public static String buildQuerySql(List<String> columnNames, String table,
            String where)
    {
        String querySql;
        StringBuilder columnBuilder = new StringBuilder();
        for (String columnName : columnNames) {
            columnBuilder.append("\"").append(columnName).append("\",");
        }
        columnBuilder.setLength(columnBuilder.length() - 1);
        if (StringUtils.isBlank(where)) {
            querySql = String.format(HBaseConstant.QUERY_SQL_TEMPLATE_WITHOUT_WHERE,
                    columnBuilder.toString(), table);
        }
        else {
            querySql = String.format(HBaseConstant.QUERY_SQL_TEMPLATE, columnBuilder.toString(),
                    table, where);
        }
        return querySql;
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
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "Addax 获取切分主键(splitPk)字段类型失败. 该错误通常是系统底层异常导致.");
        }
        return ret;
    }

    private static boolean isLongType(int type)
    {
        return type == Types.BIGINT || type == Types.INTEGER
                || type == Types.SMALLINT || type == Types.TINYINT;
    }

    private static boolean isStringType(int type)
    {
        return type == Types.CHAR || type == Types.NCHAR
                || type == Types.VARCHAR || type == Types.LONGVARCHAR
                || type == Types.NVARCHAR;
    }

    /**
     * 校验配置参数是否正确
     */
    public void validateParameter()
    {
        // query server地址必须配置
        String queryServerAddress = configuration.getNecessaryValue(HBaseKey.QUERY_SERVER_ADDRESS,
                REQUIRED_VALUE);
        String serialization = configuration.getString(HBaseKey.SERIALIZATION_NAME, HBaseConstant.DEFAULT_SERIALIZATION);
        connection = getConnection(queryServerAddress, serialization);

        //判断querySql是否配置，如果配置则table配置可为空，否则table必须配置
        querySql = configuration.getList(HBaseKey.QUERY_SQL, String.class);
        if (querySql == null || querySql.isEmpty()) {
            LOG.info("Split according to splitKey or split points.");

            String schema = configuration.getString(HBaseKey.SCHEMA, null);
            String tableName = configuration.getNecessaryValue(HBaseKey.TABLE, REQUIRED_VALUE);
            if (schema != null && !schema.isEmpty()) {
                fullTableName = "\"" + schema + "\".\"" + tableName + "\"";
            }
            else {
                fullTableName = "\"" + tableName + "\"";
            }
            // 如果列名未配置，默认读取全部列*
            columnNames = configuration.getList(HBaseKey.COLUMN, String.class);
            splitKey = configuration.getString(HBaseKey.SPLIT_KEY, null);
            splitPoints = configuration.getList(HBaseKey.SPLIT_POINT);
            checkTable(schema, tableName);
            dealWhere();
        }
        else {
            // 用户指定querySql，切分不做处理，根据给定sql读取数据即可
            LOG.info("Split according to query sql.");
        }
    }

    public Connection getConnection(String queryServerAddress, String serialization)
    {
        String url = String.format(HBaseConstant.CONNECT_STRING_TEMPLATE, queryServerAddress, serialization);
        LOG.debug("Connecting to QueryServer [{}] ...", url);
        Connection conn;
        try {
            Class.forName(HBaseConstant.CONNECT_DRIVER_STRING);
            conn = DriverManager.getConnection(url);
            conn.setAutoCommit(false);
        }
        catch (Throwable e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "无法连接QueryServer，配置不正确或服务未启动，请检查配置和服务状态或者联系HBase管理员.", e);
        }
        LOG.debug("Connected to QueryServer successfully.");
        return conn;
    }

    /**
     * 检查表名、列名和切分列是否存在
     *
     * @param schema phoenix schema
     * @param tableName phoenix table name
     */
    public void checkTable(String schema, String tableName)
    {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            String selectSql = String.format(HBaseConstant.SELECT_COLUMNS_TEMPLATE, tableName);

            // 处理schema不为空情况
            if (schema == null || schema.isEmpty()) {
                selectSql = selectSql + " AND TABLE_SCHEM IS NULL";
            }
            else {
                selectSql = selectSql + " AND TABLE_SCHEM = '" + schema + "'";
            }
            resultSet = statement.executeQuery(selectSql);
            List<String> primaryColumnNames = new ArrayList<>();
            List<String> allColumnName = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString(1);
                allColumnName.add(columnName);
                // 列族为空表示该列为主键列
                if (resultSet.getString(2) == null) {
                    primaryColumnNames.add(columnName);
                }
            }
            if (columnNames != null && !columnNames.isEmpty()) {
                for (String columnName : columnNames) {
                    if (!allColumnName.contains(columnName)) {
                        // 用户配置的列名在元数据中不存在
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                "您配置的列" + columnName + "在表" + tableName + "的元数据中不存在，请检查您的配置或者联系HBase管理员.");
                    }
                }
            }
            else {
                columnNames = allColumnName;
                configuration.set(HBaseKey.COLUMN, allColumnName);
            }
            if (splitKey != null && !primaryColumnNames.contains(splitKey)) {
                // 切分列必须是主键列，否则会严重影响读取性能
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "您配置的切分列" + splitKey + "不是表" + tableName + "的主键，请检查您的配置或者联系HBase管理员.");
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "获取表" + tableName + "信息失败，请检查您的集群和表状态或者联系HBase管理员.", e);
        }
        finally {
            closeJdbc(null, statement, resultSet);
        }
    }

    public void closeJdbc(Connection connection, Statement statement, ResultSet resultSet)
    {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOG.warn("数据库连接关闭异常. {}", CONNECT_ERROR, e);
        }
    }

    public void dealWhere()
    {
        String where = configuration.getString(HBaseKey.WHERE, null);
        if (StringUtils.isNotBlank(where)) {
            String whereImprove = where.trim();
            if (whereImprove.endsWith(";") || whereImprove.endsWith("；")) {
                whereImprove = whereImprove.substring(0, whereImprove.length() - 1);
            }
            configuration.set(HBaseKey.WHERE, whereImprove);
        }
    }

    /**
     * 对表进行切分
     *
     * @param adviceNumber the advice number of split
     * @return list of configuration
     */
    public List<Configuration> doSplit(int adviceNumber)
    {
        List<Configuration> pluginParams = new ArrayList<>();
        List<String> rangeList;
        String where = configuration.getString(HBaseKey.WHERE);
        boolean hasWhere = StringUtils.isNotBlank(where);
        if (querySql == null || querySql.isEmpty()) {
            // 如果splitPoints为空，则根据splitKey自动切分，不过这种切分方式无法保证数据均分，且只支持整形和字符型列
            if (splitPoints == null || splitPoints.isEmpty()) {
                LOG.info("Split according to the min and max value of splitColumn...");
                Pair<Object, Object> minMaxPK = getPkRange(configuration);
                if (null == minMaxPK) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "根据切分主键切分表失败. 仅支持切分主键为一个,并且类型为整数或者字符串类型. " +
                                    "请尝试使用其他的切分主键或者联系 HBase管理员 进行处理.");
                }
                if (null == minMaxPK.getLeft() || null == minMaxPK.getRight()) {
                    // 切分后获取到的start/end 有 Null 的情况
                    pluginParams.add(configuration);
                    return pluginParams;
                }
                boolean isStringType = HBaseConstant.PK_TYPE_STRING.equals(configuration
                        .getString(HBaseConstant.PK_TYPE));
                boolean isLongType = HBaseConstant.PK_TYPE_LONG.equals(configuration
                        .getString(HBaseConstant.PK_TYPE));
                if (isStringType) {
                    rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                            String.valueOf(minMaxPK.getLeft()),
                            String.valueOf(minMaxPK.getRight()), adviceNumber,
                            splitKey, "'", null);
                }
                else if (isLongType) {
                    rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                            new BigInteger(minMaxPK.getLeft().toString()),
                            new BigInteger(minMaxPK.getRight().toString()),
                            adviceNumber, splitKey);
                }
                else {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            "您配置的切分主键(splitPk) 类型不支持. 仅支持切分主键为一个,并且类型为整数或者字符串类型. " +
                                    "请尝试使用其他的切分主键或者联系HBase管理员进行处理.");
                }
            }
            else {
                LOG.info("Split according to splitPoints...");
                // 根据指定splitPoints进行切分
                rangeList = buildSplitRange();
            }
            String tempQuerySql;
            if (null != rangeList && !rangeList.isEmpty()) {
                for (String range : rangeList) {
                    Configuration tempConfig = configuration.clone();

                    tempQuerySql = buildQuerySql(columnNames, fullTableName, where)
                            + (hasWhere ? " and " : " where ") + range;
                    LOG.info("Query SQL: {}", tempQuerySql);
                    tempConfig.set(HBaseConstant.QUERY_SQL_PER_SPLIT, tempQuerySql);
                    pluginParams.add(tempConfig);
                }
            }
            else {
                Configuration tempConfig = configuration.clone();
                tempQuerySql = buildQuerySql(columnNames, fullTableName, where)
                        + (hasWhere ? " and " : " where ")
                        + String.format(" %s IS NOT NULL", splitKey);
                LOG.info("Query SQL: {}", tempQuerySql);
                tempConfig.set(HBaseConstant.QUERY_SQL_PER_SPLIT, tempQuerySql);
                pluginParams.add(tempConfig);
            }
        }
        else {
            // 指定querySql不需要切分
            for (String sql : querySql) {
                Configuration tempConfig = configuration.clone();
                tempConfig.set(HBaseConstant.QUERY_SQL_PER_SPLIT, sql);
                pluginParams.add(tempConfig);
            }
        }
        return pluginParams;
    }

    private List<String> buildSplitRange()
    {
        String getSplitKeyTypeSQL = String.format(HBaseConstant.QUERY_COLUMN_TYPE_TEMPLATE, splitKey, fullTableName);
        Statement statement = null;
        ResultSet resultSet = null;
        List<String> splitConditions = new ArrayList<>();

        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(getSplitKeyTypeSQL);
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int type = rsMetaData.getColumnType(1);
            String symbol = "%s";
            switch (type) {
                case Types.CHAR:
                case Types.VARCHAR:
                    symbol = "'%s'";
                    break;
                case Types.DATE:
                    symbol = "TO_DATE('%s')";
                    break;
                case Types.TIME:
                    symbol = "TO_TIME('%s')";
                    break;
                case Types.TIMESTAMP:
                    symbol = "TO_TIMESTAMP('%s')";
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.ARRAY:
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            "切分列类型为" + rsMetaData.getColumnTypeName(1) + "，暂不支持该类型字段作为切分列。");
                default:
                    break;
            }
            String splitCondition;
            for (int i = 0; i <= splitPoints.size(); i++) {
                if (i == 0) {
                    splitCondition = splitKey + " <= " + String.format(symbol, splitPoints.get(i));
                }
                else if (i == splitPoints.size()) {
                    splitCondition = splitKey + " > " + String.format(symbol, splitPoints.get(i - 1));
                }
                else {
                    splitCondition = splitKey + " > " + String.format(symbol, splitPoints.get(i - 1)) +
                            " AND " + splitKey + " <= " + String.format(symbol, splitPoints.get(i));
                }
                splitConditions.add(splitCondition);
            }

            return splitConditions;
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "获取切分列类型失败，请检查服务或给定表和切分列是否正常，或者联系HBase管理员进行处理。", e);
        }
        finally {
            closeJdbc(null, statement, resultSet);
        }
    }

    private Pair<Object, Object> getPkRange(Configuration configuration)
    {
        String pkRangeSQL = String.format(HBaseConstant.QUERY_MIN_MAX_TEMPLATE, splitKey, splitKey, fullTableName);
        String where = configuration.getString(HBaseKey.WHERE);
        if (StringUtils.isNotBlank(where)) {
            pkRangeSQL = String.format("%s WHERE (%s AND %s IS NOT NULL)",
                    pkRangeSQL, where, splitKey);
        }
        Statement statement = null;
        ResultSet resultSet = null;
        Pair<Object, Object> minMaxPK = null;

        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(pkRangeSQL);
            ResultSetMetaData rsMetaData = resultSet.getMetaData();

            if (isPKTypeValid(rsMetaData)) {
                if (isStringType(rsMetaData.getColumnType(1))) {
                    configuration.set(HBaseConstant.PK_TYPE, HBaseConstant.PK_TYPE_STRING);

                    if (resultSet.next()) {
                        minMaxPK = new ImmutablePair<>(
                                resultSet.getString(1), resultSet.getString(2));
                    }
                }
                else if (isLongType(rsMetaData.getColumnType(1))) {
                    configuration.set(HBaseConstant.PK_TYPE, HBaseConstant.PK_TYPE_LONG);

                    if (resultSet.next()) {
                        minMaxPK = new ImmutablePair<>(
                                resultSet.getLong(1), resultSet.getLong(2));
                    }
                }
                else {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "您配置的切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型不支持. " +
                                    "仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系HBASE管理员进行处理.");
                }
            }
            else {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "您配置的切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型不支持. " +
                                "仅支持切分主键为一个,并且类型为整数或者字符串类型. 请尝试使用其他的切分主键或者联系HBASE管理员进行处理.");
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, e);
        }
        finally {
            closeJdbc(null, statement, resultSet);
        }

        return minMaxPK;
    }
}
