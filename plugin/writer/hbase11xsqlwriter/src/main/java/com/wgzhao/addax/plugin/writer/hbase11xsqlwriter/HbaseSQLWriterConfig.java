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

import com.wgzhao.addax.core.base.HBaseConstant;
import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class HbaseSQLWriterConfig
{
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSQLWriterConfig.class);
    private Configuration originalConfig;

    private String connectionString;

    private String tableName;
    private List<String> columns;

    private NullModeType nullMode;
    private int batchSize;
    private boolean truncate;
    private boolean isThinClient;
    private String namespace;
    private String username;
    private String password;

    private boolean haveKerberos;
    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;

    private HbaseSQLWriterConfig()
    {
    }

    public static HbaseSQLWriterConfig parse(Configuration jobConf)
    {
        assert jobConf != null;
        HbaseSQLWriterConfig cfg = new HbaseSQLWriterConfig();
        cfg.originalConfig = jobConf;

        parseClusterConfig(cfg, jobConf);

        parseTableConfig(cfg, jobConf);

        cfg.nullMode = NullModeType.getByTypeName(jobConf.getString(HBaseKey.NULL_MODE, HBaseConstant.DEFAULT_NULL_MODE));
        cfg.batchSize = jobConf.getInt(HBaseKey.BATCH_SIZE, HBaseConstant.DEFAULT_BATCH_ROW_COUNT);
        cfg.truncate = jobConf.getBool(HBaseKey.TRUNCATE, HBaseConstant.DEFAULT_TRUNCATE);
        cfg.isThinClient = jobConf.getBool(HBaseKey.THIN_CLIENT, HBaseConstant.DEFAULT_USE_THIN_CLIENT);

        cfg.haveKerberos = jobConf.getBool(HBaseKey.HAVE_KERBEROS, HBaseConstant.DEFAULT_HAVE_KERBEROS);
        cfg.kerberosPrincipal = jobConf.getString(HBaseKey.KERBEROS_PRINCIPAL, HBaseConstant.DEFAULT_KERBEROS_PRINCIPAL);
        cfg.kerberosKeytabFilePath = jobConf.getString(HBaseKey.KERBEROS_KEYTAB_FILE_PATH, HBaseConstant.DEFAULT_KERBEROS_KEYTAB_FILE_PATH);

        LOG.debug("HBase SQL writer config parsed: {}", cfg);

        return cfg;
    }

    private static void parseClusterConfig(HbaseSQLWriterConfig cfg, Configuration jobConf)
    {
        String hbaseCfg = jobConf.getString(HBaseKey.HBASE_CONFIG);
        if (StringUtils.isBlank(hbaseCfg)) {
            throw AddaxException.asAddaxException(
                    REQUIRED_VALUE,
                    String.format("%s must be configured with the following:  %s and  %s",
                            HBaseKey.HBASE_CONFIG, HConstants.ZOOKEEPER_QUORUM, HConstants.ZOOKEEPER_ZNODE_PARENT));
        }

        if (jobConf.getBool(HBaseKey.THIN_CLIENT, HBaseConstant.DEFAULT_USE_THIN_CLIENT)) {
            Map<String, String> thinConnectConfig = HbaseSQLHelper.getThinConnectConfig(hbaseCfg);
            String thinConnectStr = thinConnectConfig.get(HBaseKey.HBASE_THIN_CONNECT_URL);
            cfg.namespace = thinConnectConfig.get(HBaseKey.HBASE_THIN_CONNECT_NAMESPACE);
            cfg.username = thinConnectConfig.get(HBaseKey.HBASE_THIN_CONNECT_USERNAME);
            cfg.password = thinConnectConfig.get(HBaseKey.HBASE_THIN_CONNECT_PASSWORD);
            if (Strings.isNullOrEmpty(thinConnectStr)) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        "You must configure 'hbase.thin.connect.url' if your want use thinClient mode");
            }
            if (Strings.isNullOrEmpty(cfg.namespace) || Strings.isNullOrEmpty(cfg.username) || Strings
                    .isNullOrEmpty(cfg.password)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The items 'hbase.thin.connect.namespace|username|password' must be configured if you want to use thinClient mode");
            }
            cfg.connectionString = thinConnectStr;
        }
        else {
            // parse the zk quorum and znode
            Pair<String, String> zkCfg;
            try {
                zkCfg = HbaseSQLHelper.getHbaseConfig(hbaseCfg);
            }
            catch (Throwable t) {
                // 解析hbase配置错误
                throw AddaxException.asAddaxException(
                        REQUIRED_VALUE,
                        "Failed to parse hbaseConfig，please check it.");
            }
            String zkQuorum = zkCfg.getFirst();
            String znode = zkCfg.getSecond();
            if (zkQuorum == null || zkQuorum.isEmpty() || znode == null || znode.isEmpty()) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        "The items hbase.zookeeper.quorum/zookeeper.znode.parent must be configured");
            }

            // generate phoenix jdbc url string
            // jdbc:phoenix:zk_quorum[:port]:/znode_parent[:principal:keytab]
            cfg.connectionString = "jdbc:phoenix:" + zkQuorum + ":" + znode;
        }
    }

    private static void parseTableConfig(HbaseSQLWriterConfig cfg, Configuration jobConf)
    {
        cfg.tableName = jobConf.getString(HBaseKey.TABLE);

        if (cfg.tableName == null || cfg.tableName.isEmpty()) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "The item tableName must be configured.");
        }
        try {
            TableName.valueOf(cfg.tableName);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE,
                    "The table " + cfg.tableName + " you configured has illegal symbols.");
        }

        // 解析列配置
        cfg.columns = jobConf.getList(HBaseKey.COLUMN, String.class);
        if (cfg.columns == null || cfg.columns.isEmpty()) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, "The item columns must be configured and can not be empty.");
        }
    }

    public Configuration getOriginalConfig()
    {
        return originalConfig;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public NullModeType getNullMode()
    {
        return nullMode;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    public boolean truncate()
    {
        return truncate;
    }

    public boolean isThinClient()
    {
        return isThinClient;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public String getPassword()
    {
        return password;
    }

    public String getUsername()
    {
        return username;
    }

    public boolean haveKerberos()
    {
        return haveKerberos;
    }

    public String getKerberosKeytabFilePath()
    {
        return kerberosKeytabFilePath;
    }

    public String getKerberosPrincipal()
    {
        return kerberosPrincipal;
    }

    @Override
    public String toString()
    {
        StringBuilder ret = new StringBuilder();
        ret.append("\n[jdbc]");
        ret.append(connectionString);
        ret.append("\n");

        // 表配置
        ret.append("[tableName]");
        ret.append(tableName);
        ret.append("\n");
        ret.append("[column]");
        for (String col : columns) {
            ret.append(col);
            ret.append(",");
        }
        ret.setLength(ret.length() - 1);
        ret.append("\n");

        ret.append("[nullMode]");
        ret.append(nullMode);
        ret.append("\n");
        ret.append("[batchSize]");
        ret.append(batchSize);
        ret.append("\n");
        ret.append("[truncate]");
        ret.append(truncate);
        ret.append("\n");

        return ret.toString();
    }
}
