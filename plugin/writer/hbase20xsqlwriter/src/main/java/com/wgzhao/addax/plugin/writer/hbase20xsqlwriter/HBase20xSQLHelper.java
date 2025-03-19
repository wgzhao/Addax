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

package com.wgzhao.addax.plugin.writer.hbase20xsqlwriter;

import com.wgzhao.addax.core.base.HBaseConstant;
import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class HBase20xSQLHelper
{

    // phoenix thin driver name
    public static final String PHOENIX_JDBC_THIN_DRIVER = "org.apache.phoenix.queryserver.client.Driver";
    // phoenix jdbc driver name
    public static final String PHOENIX_JDBC_THICK_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String SELECT_CATALOG_TABLE_STRING = "SELECT COLUMN_NAME FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_NAME IS NOT NULL";
    private static final Logger LOG = LoggerFactory.getLogger(HBase20xSQLHelper.class);

    private HBase20xSQLHelper() {}

    /*
     * 验证配置参数是否正确
     */
    public static void validateParameter(Configuration originalConfig)
    {
        String tableName = originalConfig.getNecessaryValue(HBaseKey.TABLE, REQUIRED_VALUE);
        String jdbcUrl = originalConfig.getNecessaryValue(HBaseKey.JDBC_URL, REQUIRED_VALUE);
        boolean isThinMode = jdbcUrl.contains(":thin:");
        String serialization = originalConfig.getString(HBaseKey.SERIALIZATION_NAME, HBaseConstant.DEFAULT_SERIALIZATION);
        String connStr = jdbcUrl;
        if (isThinMode) {
            connStr = connStr + ";serialization=" + serialization;
        }
        // check kerberos
        if (originalConfig.getBool(HBaseKey.HAVE_KERBEROS, false)) {
            String principal = originalConfig.getNecessaryValue(HBaseKey.KERBEROS_PRINCIPAL, REQUIRED_VALUE);
            String keytab = originalConfig.getNecessaryValue(HBaseKey.KERBEROS_KEYTAB_FILE_PATH, REQUIRED_VALUE);
            kerberosAuthentication(principal, keytab);
            if (isThinMode) {
                connStr = connStr + "authentication=SPENGO;principal=" + principal + ";keytab=" + keytab;
            } else {
                connStr = connStr + ":" + principal + ":" + keytab;
            }
        }
        Connection conn;
        if (jdbcUrl.contains(":thin:")) {
             conn = getClientConnection(connStr, PHOENIX_JDBC_THIN_DRIVER);
        } else {
            conn = getClientConnection(connStr, PHOENIX_JDBC_THICK_DRIVER);
        }

        List<String> columnNames = originalConfig.getList(HBaseKey.COLUMN, String.class);
        if (columnNames == null || columnNames.isEmpty()) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR, "The column configuration is empty, please check the configuration.");
        }
        String schema = originalConfig.getString(HBaseKey.SCHEMA);
        checkTable(conn, schema, tableName, columnNames);
        // rewrite queryServerAddress with extra properties
        originalConfig.set(HBaseKey.QUERY_SERVER_ADDRESS, connStr);
    }

    public static Connection getClientConnection(String connStr, String driverName)
    {
        LOG.debug("Connecting to QueryServer [{}] ...", connStr);
        Connection conn;
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(connStr);
            conn.setAutoCommit(false);
        }
        catch (Throwable e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to connect the QueryServer", e);
        }
        LOG.debug("Connected to QueryServer successfully.");
        return conn;
    }

    public static Connection getJdbcConnection(Configuration conf)
    {
        String queryServerAddress = conf.getNecessaryValue(HBaseKey.QUERY_SERVER_ADDRESS, REQUIRED_VALUE);
        if (queryServerAddress.contains(":thin:")) {
            return getClientConnection(queryServerAddress, PHOENIX_JDBC_THIN_DRIVER);
        } else {
            return getClientConnection(queryServerAddress, PHOENIX_JDBC_THICK_DRIVER);
        }
    }

    public static void checkTable(Connection conn, String schema, String tableName, List<String> columnNames)
    {
        String selectSystemTable = getSelectSystemSQL(schema, tableName);
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(selectSystemTable);
            List<String> allColumns = new ArrayList<>();
            if (rs.next()) {
                allColumns.add(rs.getString(1));
            }
            else {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "The table " + tableName + " does not exist, please check the configuration.");
            }
            while (rs.next()) {
                allColumns.add(rs.getString(1));
            }
            for (String columnName : columnNames) {
                if (!allColumns.contains(columnName)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The column " + columnName + " does not exists in the table " + tableName);
                }
            }
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,"Failed to get the table information of " + tableName, e);
        }
        finally {
            closeJdbc(conn, st, rs);
        }
    }

    private static String getSelectSystemSQL(String schema, String tableName)
    {
        String sql = String.format(SELECT_CATALOG_TABLE_STRING, tableName);
        if (schema != null) {
            sql = sql + " AND TABLE_SCHEM = '" + schema + "'";
        }
        return sql;
    }

    public static void closeJdbc(Connection connection, Statement statement, ResultSet resultSet)
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
            LOG.warn("Failed to close the jdbc connection", e);
        }
    }

    /**
     * Try to authentication with kerberos
     *
     * @param kerberosPrincipal the principal to Kerberos
     * @param kerberosKeytabFilePath the keytab filepath
     */
    private static void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        }
        catch (Exception e) {
            String message = String.format("Kerberos authentication failed, please make sure that kerberosKeytabFilePath[%s] and kerberosPrincipal[%s] are correct",
                    kerberosKeytabFilePath, kerberosPrincipal);
            LOG.error(message);
            throw AddaxException.asAddaxException(LOGIN_ERROR, e);
        }
    }
}
