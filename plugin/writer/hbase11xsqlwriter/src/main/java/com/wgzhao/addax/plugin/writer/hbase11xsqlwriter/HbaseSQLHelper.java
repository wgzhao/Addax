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

package com.wgzhao.addax.plugin.writer.hbase11xsqlwriter;

import com.wgzhao.addax.common.base.HBaseConstant;
import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.PERMISSION_ERROR;

public class HbaseSQLHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSQLHelper.class);

    public static ThinClientPTable pTable;
    public static final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    // Kerberos
    private static boolean haveKerberos = false;
    private static String kerberosKeytabFilePath;
    private static String kerberosPrincipal;

    private HbaseSQLHelper() {}

    public static HbaseSQLWriterConfig parseConfig(Configuration cfg)
    {
        return HbaseSQLWriterConfig.parse(cfg);
    }

    public static Pair<String, String> getHbaseConfig(String hbaseCfgString)
    {
        assert hbaseCfgString != null;
        Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseCfgString, new TypeReference<Map<String, String>>() {});
        String zkQuorum = hbaseConfigMap.get(HConstants.ZOOKEEPER_QUORUM);
        if (!zkQuorum.contains(":")) {
            zkQuorum = zkQuorum + ":2181";
        }
        String znode = hbaseConfigMap.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
        if (znode == null || znode.isEmpty()) {
            znode = HBaseConstant.DEFAULT_ZNODE;
        }
        return new Pair<>(zkQuorum, znode);
    }

    public static Map<String, String> getThinConnectConfig(String hbaseCfgString)
    {
        assert hbaseCfgString != null;
        return JSON.parseObject(hbaseCfgString, new TypeReference<Map<String, String>>() {});
    }

    public static void validateConfig(HbaseSQLWriterConfig cfg)
    {
        Connection conn = getJdbcConnection(cfg);

        checkTable(conn, cfg.getNamespace(), cfg.getTableName(), cfg.isThinClient());

        PTable schema;
        try {
            schema = getTableSchema(conn, cfg.getNamespace(), cfg.getTableName(), cfg.isThinClient());
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Unable to get the metadata of table " + cfg.getTableName(), e);
        }
        List<String> columnNames = cfg.getColumns();
        try {

            for (String colName : columnNames) {
                schema.getColumnForColumnName(colName);
            }
        }
        catch (ColumnNotFoundException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The column '" + e.getColumnName() + "' your configured does not exists in the target table " + cfg.getTableName(), e);
        }
        catch (SQLException e) {
            // 列名有二义性或者其他问题
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The column validation of target table " + cfg.getTableName() + "has got failure", e);
        }
    }

    public static Connection getJdbcConnection(HbaseSQLWriterConfig cfg)
    {
        String connStr = cfg.getConnectionString();
        LOG.debug("Connecting to HBase cluster [{}] ...", connStr);
        Connection conn;
        haveKerberos = cfg.haveKerberos();
        if (haveKerberos) {
            kerberosKeytabFilePath = cfg.getKerberosKeytabFilePath();
            kerberosPrincipal = cfg.getKerberosPrincipal();
            hadoopConf.set("hadoop.security.authentication", "Kerberos");
        }
        kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath);
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            if (cfg.isThinClient()) {
                conn = getThinClientJdbcConnection(cfg);
            }
            else {
                conn = DriverManager.getConnection(connStr);
            }
            conn.setAutoCommit(false);
        }
        catch (Throwable e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Unable to connect to hbase cluster, please check the configuration and cluster status ", e);
        }
        LOG.debug("Connected to HBase cluster successfully.");
        return conn;
    }

    private static void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        if (haveKerberos && StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
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

    public static Connection getThinClientJdbcConnection(HbaseSQLWriterConfig cfg)
            throws SQLException
    {
        String connStr = cfg.getConnectionString();
        LOG.info("Connecting to HBase cluster [{}] use thin client ...", connStr);
        Connection conn = DriverManager.getConnection(connStr, cfg.getUsername(), cfg.getPassword());
        String userNamespaceQuery = "use " + cfg.getNamespace();
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(userNamespaceQuery);
            return conn;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Can not connection to the namespace.", e);
        }
    }

    public static PTable getTableSchema(Connection conn, String fullTableName)
            throws SQLException
    {
        PhoenixConnection hconn = conn.unwrap(PhoenixConnection.class);
        MetaDataClient mdc = new MetaDataClient(hconn);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
        return mdc.updateCache(schemaName, tableName).getTable();
    }

    public static PTable getTableSchema(Connection conn, String namespace, String fullTableName, boolean isThinClient)
            throws
            SQLException
    {
        LOG.info("Start to get table schema of namespace={}, fullTableName={}", namespace, fullTableName);
        if (!isThinClient) {
            return getTableSchema(conn, fullTableName);
        }
        else {
            if (pTable == null) {
                try (ResultSet result = conn.getMetaData().getColumns(null, namespace, fullTableName, null)) {
                    ThinClientPTable retTable = new ThinClientPTable();
                    retTable.setColTypeMap(parseColType(result));
                    pTable = retTable;
                }
            }
            return pTable;
        }
    }

    public static Map<String, ThinClientPTable.ThinClientPColumn> parseColType(ResultSet rs)
            throws SQLException
    {
        Map<String, ThinClientPTable.ThinClientPColumn> cols = new HashMap<>();
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();

        while (rs.next()) {
            String colName = null;
            PDataType colType = null;
            for (int i = 1; i <= columnCount; i++) {
                if ("TYPE_NAME".equals(md.getColumnLabel(i))) {
                    colType = PDataType.fromSqlTypeName((String) rs.getObject(i));
                }
                else if ("COLUMN_NAME".equals(md.getColumnLabel(i))) {
                    colName = (String) rs.getObject(i);
                }
            }
            if (colType == null || colName == null) {
                throw new SQLException("ColType or colName is null, colType : " + colType + " , colName : " + colName);
            }
            cols.put(colName, new ThinClientPTable.ThinClientPColumn(colName, colType));
        }
        return cols;
    }

    public static void truncateTable(Connection conn, String tableName)
    {
        PhoenixConnection sqlConn;
        Admin admin = null;
        try {
            sqlConn = conn.unwrap(PhoenixConnection.class);
            admin = sqlConn.getQueryServices().getAdmin();
            TableName hTableName = getTableName(tableName);
            checkTable(admin, hTableName);
            admin.disableTable(hTableName);
            admin.truncateTable(hTableName, true);
            LOG.debug("Table {} has been truncated.", tableName);
        }
        catch (Throwable t) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "Failed to truncate " + tableName + ".", t);
        }
        finally {
            if (admin != null) {
                closeAdmin(admin);
            }
        }
    }

    public static void checkTable(Connection conn, String namespace, String tableName, boolean isThinClient)
    {
        //ignore check table when use thin client
        if (!isThinClient) {
            checkTable(conn, tableName);
        }
    }

    public static void checkTable(Connection conn, String tableName)
    {
        PhoenixConnection sqlConn;
        Admin admin = null;
        try {
            sqlConn = conn.unwrap(PhoenixConnection.class);
            admin = sqlConn.getQueryServices().getAdmin();
            checkTable(admin, getTableName(tableName));
        }
        catch (SQLException | IOException t) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "The table " + tableName + "status check failed, please check the HBase cluster status.", t);
        }
        finally {
            if (admin != null) {
                closeAdmin(admin);
            }
        }
    }

    private static void checkTable(Admin admin, TableName tableName)
            throws IOException
    {
        if (!admin.tableExists(tableName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The hbase table " + tableName + "doest not exists.");
        }
        if (!admin.isTableAvailable(tableName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The hbase table" + tableName + "is unavailable.");
        }
        if (admin.isTableDisabled(tableName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The hbase table " + tableName + "is disabled at current, please enable it before using");
        }
        LOG.info("table {} exists", tableName);
    }

    private static void closeAdmin(Admin admin)
    {
        try {
            if (null != admin) {
                admin.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(PERMISSION_ERROR, e);
        }
    }

    private static TableName getTableName(String tableName)
    {
        if (tableName.contains(".")) {
            tableName = tableName.replace(".", ":");
        }
        return TableName.valueOf(tableName);
    }
}
