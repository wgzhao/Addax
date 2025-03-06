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
import com.wgzhao.addax.rdbms.reader.util.ReaderSplitUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class HBase20SQLReaderHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HBase20SQLReaderHelper.class);

    private final Configuration configuration;

    private Connection connection;
    private List<String> columnNames;
    private String splitKey;

    public HBase20SQLReaderHelper(Configuration configuration)
    {
        this.configuration = configuration;
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
        List<String> querySql = configuration.getList(HBaseKey.QUERY_SQL, String.class);
        if (querySql == null || querySql.isEmpty()) {
            LOG.info("Split according to splitKey or split points.");

            String schema = configuration.getString(HBaseKey.SCHEMA, null);
            String tableName = configuration.getNecessaryValue(HBaseKey.TABLE, REQUIRED_VALUE);
            String fullTableName;
            if (schema != null && !schema.isEmpty()) {
                fullTableName = "\"" + schema + "\".\"" + tableName + "\"";
            }
            else {
                fullTableName = "\"" + tableName + "\"";
            }
            configuration.set(HBaseKey.TABLE, fullTableName);
            // 如果列名未配置，默认读取全部列*
            columnNames = configuration.getList(HBaseKey.COLUMN, String.class);
            splitKey = configuration.getString(HBaseKey.SPLIT_KEY, null);
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
        List<Configuration> splitResultConfigs = new ArrayList<>();
        for (int j = 0; j < adviceNumber; j++) {
            splitResultConfigs.add(configuration.clone());
        }
        return splitResultConfigs;
    }
}
