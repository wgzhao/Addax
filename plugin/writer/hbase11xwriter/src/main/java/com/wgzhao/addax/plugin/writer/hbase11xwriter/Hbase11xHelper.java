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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.wgzhao.addax.common.base.HBaseConstant;
import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class Hbase11xHelper
{

    private static final Logger LOG = LoggerFactory.getLogger(Hbase11xHelper.class);

    private Hbase11xHelper() {}

    public static org.apache.hadoop.conf.Configuration getHbaseConfiguration(String hbaseConfig)
    {
        if (StringUtils.isBlank(hbaseConfig)) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "The item hbaseConfig must be configured.");
        }
        org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
        try {
            Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseConfig, new TypeReference<Map<String, String>>() {});
            // 用户配置的 key-value 对 来表示 hbaseConfig
            Validate.isTrue(hbaseConfigMap != null, "The item hbaseConfig must be not empty.");
            for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
                hConfiguration.set(entry.getKey(), entry.getValue());
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.HBASE_CONNECTION_ERROR, e);
        }
        return hConfiguration;
    }

    public static org.apache.hadoop.hbase.client.Connection getHbaseConnection(String hbaseConfig)
    {
        org.apache.hadoop.conf.Configuration hConfiguration = Hbase11xHelper.getHbaseConfiguration(hbaseConfig);

        org.apache.hadoop.hbase.client.Connection hConnection = null;
        try {
            hConnection = ConnectionFactory.createConnection(hConfiguration);
        }
        catch (Exception e) {
            Hbase11xHelper.closeConnection(hConnection);
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.HBASE_CONNECTION_ERROR, e);
        }
        return hConnection;
    }

    public static Table getTable(Configuration configuration)
    {
        String hbaseConfig = configuration.getString(HBaseKey.HBASE_CONFIG);
        String userTable = configuration.getString(HBaseKey.TABLE);
        long writeBufferSize = configuration.getLong(HBaseKey.WRITE_BUFFER_SIZE, HBaseConstant.DEFAULT_WRITE_BUFFER_SIZE);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        org.apache.hadoop.hbase.client.Table hTable = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin, hTableName);
            hTable = hConnection.getTable(hTableName);
            BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(hTableName);
            bufferedMutatorParams.writeBufferSize(writeBufferSize);
        }
        catch (Exception e) {
            Hbase11xHelper.closeTable(hTable);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.HBASE_TABLE_ERROR, e);
        }
        return hTable;
    }

    public static BufferedMutator getBufferedMutator(Configuration configuration)
    {
        String hbaseConfig = configuration.getString(HBaseKey.HBASE_CONFIG);
        String userTable = configuration.getString(HBaseKey.TABLE);
        long writeBufferSize = configuration.getLong(HBaseKey.WRITE_BUFFER_SIZE, HBaseConstant.DEFAULT_WRITE_BUFFER_SIZE);
        org.apache.hadoop.conf.Configuration hConfiguration = Hbase11xHelper.getHbaseConfiguration(hbaseConfig);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        BufferedMutator bufferedMutator = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin, hTableName);
            //参考HTable getBufferedMutator()
            bufferedMutator = hConnection.getBufferedMutator(
                    new BufferedMutatorParams(hTableName)
                            .pool(HTable.getDefaultExecutor(hConfiguration))
                            .writeBufferSize(writeBufferSize));
        }
        catch (Exception e) {
            Hbase11xHelper.closeBufferedMutator(bufferedMutator);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.GET_HBASE_BUFFERED_MUTATOR_ERROR, e);
        }
        return bufferedMutator;
    }

    public static void truncateTable(Configuration configuration)
    {
        String hbaseConfig = configuration.getString(HBaseKey.HBASE_CONFIG);
        String userTable = configuration.getString(HBaseKey.TABLE);
        LOG.info("Begin truncating table {} .", userTable);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        org.apache.hadoop.hbase.client.Admin admin = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin, hTableName);
            admin.disableTable(hTableName);
            admin.truncateTable(hTableName, true);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.TRUNCATE_HBASE_ERROR, e);
        }
        finally {
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
        }
    }

    public static void closeConnection(Connection hConnection)
    {
        try {
            if (null != hConnection) {
                hConnection.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.CLOSE_HBASE_CONNECTION_ERROR, e);
        }
    }

    public static void closeAdmin(Admin admin)
    {
        try {
            if (null != admin) {
                admin.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.CLOSE_HBASE_AMIN_ERROR, e);
        }
    }

    public static void closeBufferedMutator(BufferedMutator bufferedMutator)
    {
        try {
            if (null != bufferedMutator) {
                bufferedMutator.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.CLOSE_HBASE_BUFFERED_MUTATOR_ERROR, e);
        }
    }

    public static void closeTable(Table table)
    {
        try {
            if (null != table) {
                table.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.CLOSE_HBASE_TABLE_ERROR, e);
        }
    }

    private static void checkHbaseTable(Admin admin, TableName hTableName)
            throws IOException
    {
        if (!admin.tableExists(hTableName)) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The table " + hTableName.toString() + "DOES NOT exists.");
        }
        if (!admin.isTableAvailable(hTableName)) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The table " + hTableName.toString() + " is unavailable.");
        }
        if (admin.isTableDisabled(hTableName)) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The table " + hTableName.toString() + "is disabled,");
        }
    }

    public static void validateParameter(Configuration originalConfig)
    {
        originalConfig.getNecessaryValue(HBaseKey.HBASE_CONFIG, Hbase11xWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(HBaseKey.TABLE, Hbase11xWriterErrorCode.REQUIRED_VALUE);

        Hbase11xHelper.validateMode(originalConfig);

        String encoding = originalConfig.getString(HBaseKey.ENCODING, HBaseConstant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The encoding[" + encoding + "] is unsupported ");
        }
        originalConfig.set(HBaseKey.ENCODING, encoding);

        // validate kerberos login
        if (originalConfig.getBool(Key.HAVE_KERBEROS, false)) {
            String principal = originalConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            String keytab = originalConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            kerberosAuthentication(principal, keytab);
        }
        Boolean walFlag = originalConfig.getBool(HBaseKey.WAL_FLAG, false);
        originalConfig.set(HBaseKey.WAL_FLAG, walFlag);
        long writeBufferSize = originalConfig.getLong(HBaseKey.WRITE_BUFFER_SIZE, HBaseConstant.DEFAULT_WRITE_BUFFER_SIZE);
        originalConfig.set(HBaseKey.WRITE_BUFFER_SIZE, writeBufferSize);
    }

    private static void validateMode(Configuration originalConfig)
    {
        String mode = originalConfig.getNecessaryValue(HBaseKey.MODE, Hbase11xWriterErrorCode.REQUIRED_VALUE);
        ModeType modeType = ModeType.getByTypeName(mode);
        if (modeType == ModeType.NORMAL) {
            validateRowkeyColumn(originalConfig);
            validateColumn(originalConfig);
            validateVersionColumn(originalConfig);
        }
        else {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The mode " + mode + "is unsupported");
        }
    }

    private static void validateColumn(Configuration originalConfig)
    {
        List<Configuration> columns = originalConfig.getListConfiguration(HBaseKey.COLUMN);
        if (columns == null || columns.isEmpty()) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.REQUIRED_VALUE,
                    "The item column must be configured, the form is 'column:[{\"index\": 0,\"name\": \"cf0:column0\",\"type\": " +
                            "\"string\"},{\"index\": 1,\"name\": \"cf1:column1\",\"type\": \"long\"}]'");
        }
        for (Configuration aColumn : columns) {
            Integer index = aColumn.getInt(HBaseKey.INDEX);
            String type = aColumn.getNecessaryValue(HBaseKey.TYPE, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            String name = aColumn.getNecessaryValue(HBaseKey.NAME, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if (name.split(":").length != 2) {
                throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                        String.format("The field's name[%s] is not valid, it must be configured as cf:qualifier", name));
            }
            if (index == null || index < 0) {
                throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The index of name is not valid, it must be non-negative number");
            }
        }
    }

    private static void validateRowkeyColumn(Configuration originalConfig)
    {
        List<Configuration> rowkeyColumn = originalConfig.getListConfiguration(HBaseKey.ROW_KEY_COLUMN);
        if (rowkeyColumn == null || rowkeyColumn.isEmpty()) {
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.REQUIRED_VALUE,
                    "The item rowkeyColumn is required，such as rowkeyColumn:[{\"index\": 0,\"type\": \"string\"},{\"index\": -1,\"type\": \"string\",\"value\": \"_\"}]");
        }
        int rowkeyColumnSize = rowkeyColumn.size();
        for (Configuration aRowkeyColumn : rowkeyColumn) {
            Integer index = aRowkeyColumn.getInt(HBaseKey.INDEX);
            String type = aRowkeyColumn.getNecessaryValue(HBaseKey.TYPE, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            ColumnType.getByTypeName(type);
            if (index == null) {
                throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "The index of rowkeyColumn is required");
            }
            //不能只有-1列,即rowkey连接串
            if (rowkeyColumnSize == 1 && index == -1) {
                throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                        "The item rowkeyColumn can not all be constant, it must be specify more than one rowkey column");
            }
            if (index == -1) {
                aRowkeyColumn.getNecessaryValue(HBaseKey.VALUE, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            }
        }
    }

    private static void validateVersionColumn(Configuration originalConfig)
    {
        Configuration versionColumn = originalConfig.getConfiguration(HBaseKey.VERSION_COLUMN);
        //为null,表示用当前时间;指定列,需要index
        if (versionColumn != null) {
            Integer index = versionColumn.getInt(HBaseKey.INDEX);
            if (index == null) {
                throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.REQUIRED_VALUE, "The field[index] of versionColumn is required.");
            }
            if (index == -1) {
                //指定时间,需要index=-1,value
                versionColumn.getNecessaryValue(HBaseKey.VALUE, Hbase11xWriterErrorCode.REQUIRED_VALUE);
            }
            else if (index < 0) {
                throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "The field[index] of versionColumn must be either -1 or non-negative number");
            }
        }
    }

    public static void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath)
    {
        LOG.debug("Try to login in with principal[{}] and keytab[{}]", kerberosPrincipal, kerberosKeytabFilePath);
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        }
        catch (IOException e) {
            String message = String.format("Kerberos authentication failed, please make sure that kerberosKeytabFilePath[%s] and kerberosPrincipal[%s] are correct",
                    kerberosKeytabFilePath, kerberosPrincipal);
            LOG.error(message);
            throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
        }
    }
}
