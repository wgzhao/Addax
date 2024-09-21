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

package com.wgzhao.addax.plugin.reader.hbase11xsqlreader;

import com.wgzhao.addax.common.base.HBaseConstant;
import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.exception.CommonErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.REQUIRED_VALUE;

public class HbaseSQLHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSQLHelper.class);
    //    private static final String JDBC_PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    private final Configuration jobConf;

    public HbaseSQLHelper(Configuration conf)
    {
        this.jobConf = conf;
    }

    public Configuration parseConfig()
    {
        // 获取hbase集群的连接信息字符串
        Map<String, Object> hbaseCfg = jobConf.getMap(HBaseKey.HBASE_CONFIG);
        String zkUrl;
        if (hbaseCfg == null || hbaseCfg.isEmpty()) {
            // 集群配置必须存在且不为空
            throw AddaxException.asAddaxException(
                    REQUIRED_VALUE,
                    String.format("%s must be configured with the following:  %s and  %s",
                            HBaseKey.HBASE_CONFIG, HConstants.ZOOKEEPER_QUORUM, HConstants.ZOOKEEPER_ZNODE_PARENT));
        }
        String table = jobConf.getString(Key.TABLE);
        String querySql = jobConf.getString(Key.QUERY_SQL);
        List<String> columns = jobConf.getList(Key.COLUMN, String.class);
        if (table == null && querySql == null) {
            throw AddaxException.asAddaxException(
                    REQUIRED_VALUE,
                    String.format("The %s and %s must have a configuration", Key.TABLE, Key.QUERY_SQL)
            );
        }

        if (table != null && querySql != null) {
            LOG.warn("Both {} and {} are configured, preferring the latter", Key.TABLE, Key.QUERY_SQL);
        }
        // check columns
        if (columns == null) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "The column configuration contains illegal chars, please check them");
        }

        String zkQuorum = hbaseCfg.getOrDefault(HConstants.ZOOKEEPER_QUORUM, "").toString();
        String znode = hbaseCfg.getOrDefault(HConstants.ZOOKEEPER_ZNODE_PARENT, HBaseConstant.DEFAULT_ZNODE).toString();

        if (zkQuorum.isEmpty()) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "The " + HConstants.ZOOKEEPER_QUORUM + " can not be set to empty");
        }
        if (!znode.startsWith("/")) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "The " + HConstants.ZOOKEEPER_ZNODE_PARENT + " must be start with /"
            );
        }

        if (zkQuorum.contains(":")) {
            // Has zookeeper port
            zkUrl = zkQuorum + ":" + znode;
        }
        else {
            // Uses default zookeeper port
            zkUrl = String.format("%s:%s:%s", zkQuorum, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT, znode);
        }
        // 生成sql使用的连接字符串， 格式： jdbc:hbase:zk_quorum:2181:/znode_parent:[principal:keytab]
        String jdbcUrl = "jdbc:phoenix:" + zkUrl;
        // has kerberos ?
        if (jobConf.getBool(Key.HAVE_KERBEROS, false)) {
            String principal = jobConf.getString(Key.KERBEROS_PRINCIPAL);
            String keytab = jobConf.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            if (principal == null || keytab == null) {
                throw AddaxException.asAddaxException(
                        REQUIRED_VALUE,
                        "To enable kerberos, you must both configure " + Key.KERBEROS_PRINCIPAL + " and " + Key.KERBEROS_KEYTAB_FILE_PATH
                );
            }
            // login with kerberos
            kerberosAuthentication(principal, keytab);
            jdbcUrl = jdbcUrl + ":" + principal + ":" + keytab;
            LOG.debug("Connect to HBase cluster successfully.");
        }
        jobConf.set(Key.JDBC_URL, jdbcUrl);
        if (querySql == null) {
            generateQuerySql(table, columns, jdbcUrl);
        }
        return jobConf;
    }

    /**
     * 依据三个不同配置场景生成正确的查询语句
     *
     * @param table 表名
     * @param columns 字段
     * @param url jdbc url
     */
    private void generateQuerySql(String table, List<String> columns, String url)
    {
        if (columns.isEmpty() || (columns.size() == 1 && "*".equals(columns.get(0)))) {
            // get columns from
            columns = getPColumnNames(table, url);
        }
        jobConf.set(Key.COLUMN, columns);
        String where = jobConf.getString(Key.WHERE, null);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(String.join(", ", columns));
        sql.append(" FROM ").append(table);
        if (where != null && !where.isEmpty()) {
            sql.append(" WHERE ").append(where);
        }
        jobConf.set(Key.QUERY_SQL, sql.toString());
    }

    public List<String> getPColumnNames(String fullTableName, String url)
    {
        LOG.info("column is not configured, try to retrieve column description from hbase table");

        try (Connection conn = DriverManager.getConnection(url)) {
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
            MetaDataClient metaDataClient = new MetaDataClient(phoenixConnection);
            String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
            PTable table = metaDataClient.updateCache(schemaName, tableName).getTable();
            List<String> columnNames = new ArrayList<>();
            for (PColumn pColumn : table.getColumns()) {
                if (!pColumn.getName().getString().equals(SaltingUtil.SALTING_COLUMN_NAME)) {
                    columnNames.add(pColumn.getName().getString());
                }
                else {
                    LOG.info("{} is salt table", tableName);
                }
            }
            LOG.info("End retrieve column description");
            return columnNames;
        }
        catch (SQLException e) {
            throw AddaxException.asAddaxException(
                    EXECUTE_FAIL, "Failed to get table's column description:\n" + e.getMessage(), e);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        hadoopConf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        }
        catch (Exception e) {
            String message = String.format("kerberos authentication failed, please make sure that kerberosKeytabFilePath[%s] and kerberosPrincipal[%s] are configure correctly",
                    kerberosKeytabFilePath, kerberosPrincipal);
            LOG.error(message);
            throw AddaxException.asAddaxException(LOGIN_ERROR, e);
        }
    }
}
