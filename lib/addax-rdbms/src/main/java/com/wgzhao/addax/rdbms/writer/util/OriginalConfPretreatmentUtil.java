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

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.util.EncryptUtil;
import com.wgzhao.addax.common.util.ListUtil;
import com.wgzhao.addax.rdbms.util.ConnectionFactory;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.util.JdbcConnectionFactory;
import com.wgzhao.addax.rdbms.util.TableExpandUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.ErrorCode.REQUIRED_VALUE;

public final class OriginalConfPretreatmentUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType dataBaseType;

    private OriginalConfPretreatmentUtil() {}

    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType)
    {
        // 检查 username 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, REQUIRED_VALUE);

        // 有些数据库没有密码，因此密码不再作为必选项
        if (originalConfig.getString(Key.PASSWORD) == null) {
            originalConfig.set(Key.PASSWORD, "");
        }
        else if (originalConfig.getString(Key.PASSWORD).startsWith(Constant.ENC_PASSWORD_PREFIX)) {
            // encrypted password, need to decrypt
            String pass = originalConfig.getString(Key.PASSWORD);
            String decryptPassword = EncryptUtil.decrypt(pass.substring(6, pass.length() - 1));
            originalConfig.set(Key.PASSWORD, decryptPassword);
        }
        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        dealColumnConf(originalConfig);
        dealWriteMode(originalConfig, dataBaseType);
    }

    public static void doCheckBatchSize(Configuration originalConfig)
    {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, String.format(
                    "The item batchSize [%s] must be greater than 1. recommended value range is [100,1000].",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    public static void simplifyConf(Configuration originalConfig)
    {
        List<Configuration> connections = originalConfig.getListConfiguration(Key.CONNECTION);
        int tableNum = 0;

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = connections.get(i);
            // 是否配置的定制的驱动名称
            String driverClass = connConf.getString(Key.JDBC_DRIVER, null);
            if (driverClass != null && !driverClass.isEmpty()) {
                LOG.warn("Use specified driver class [{}]", driverClass);
                dataBaseType.setDriverClassName(driverClass);
            }
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The item jdbcUrl is required.");
            }

            jdbcUrl = dataBaseType.appendJDBCSuffixForWriter(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Key.CONNECTION, i, Key.JDBC_URL), jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The item table is required.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil.expandTableConf(dataBaseType, tables);

            if (expandedTables.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,  "The item table is required.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Key.CONNECTION, i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, String oneTable)
    {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The item column is required and can not be empty.");
        }
        else {
            boolean isPreCheck = originalConfig.getBool(Key.DRY_RUN, false);
            List<String> allColumns;
            if (isPreCheck) {
                allColumns = DBUtil.getTableColumnsByConn(connectionFactory.getConnectionWithoutRetry(), oneTable);
            }
            else {
                allColumns = DBUtil.getTableColumnsByConn(connectionFactory.getConnection(), oneTable);
            }

            LOG.info("The table [{}] has columns [{}].", oneTable, StringUtils.join(allColumns, ","));

            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("There are some risks in the column configuration. Because you did not configure the columns " +
                        "to read the database table, changes in the number and types of fields in your table may affect " +
                        "the correctness of the task or even cause errors.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            }
            else if (userConfiguredColumns.size() > allColumns.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        String.format("The number of columns your configured [%d] are greater than the number of table columns [%d].",
                                userConfiguredColumns.size(), allColumns.size()));
            }
            else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);
                Connection connection = null;
                try {
                    // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                    connection = connectionFactory.getConnection();
                    DBUtil.getColumnMetaData(connection, oneTable, StringUtils.join(userConfiguredColumns, ","));
                } finally {
                    DBUtil.closeDBResources(null, null, connection);
                }
            }
        }
    }

    public static void dealColumnConf(Configuration originalConfig)
    {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s", Key.CONNECTION, Key.JDBC_URL));
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String oneTable = originalConfig.getString(String.format("%s[0].%s[0]", Key.CONNECTION, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(dataBaseType, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    public static void dealWriteMode(Configuration originalConfig, DataBaseType dataBaseType)
    {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s", Key.CONNECTION, Key.JDBC_URL));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, false);

        LOG.info("Writing data using [{}].", writeDataSqlTemplate);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }
}
