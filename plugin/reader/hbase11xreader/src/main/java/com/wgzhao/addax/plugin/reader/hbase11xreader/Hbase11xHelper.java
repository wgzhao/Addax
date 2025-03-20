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

package com.wgzhao.addax.plugin.reader.hbase11xreader;

import com.wgzhao.addax.core.base.HBaseConstant;
import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;


public class Hbase11xHelper
{

    private static final Logger LOG = LoggerFactory.getLogger(Hbase11xHelper.class);
    private static org.apache.hadoop.hbase.client.Connection hConnection = null;

    private Hbase11xHelper() {}

    public static org.apache.hadoop.hbase.client.Connection getHbaseConnection(String hbaseConfig)
    {
        if (hConnection != null && !hConnection.isClosed()) {
            return hConnection;
        }
        if (StringUtils.isBlank(hbaseConfig)) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "hbaseConfig is required");
        }
        org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
        try {
            Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseConfig, new TypeReference<Map<String, String>>() {});
            // 用户配置的 key-value 对 来表示 hbaseConfig
            Validate.isTrue(hbaseConfigMap != null && !hbaseConfigMap.isEmpty(), "hbaseConfig can not be empty.");
            for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet())  //NOSONAR
            {
                hConfiguration.set(entry.getKey(), entry.getValue());
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, e);
        }
        try {
            hConnection = ConnectionFactory.createConnection(hConfiguration);
        }
        catch (Exception e) {
            Hbase11xHelper.closeConnection(hConnection);
            throw AddaxException.asAddaxException(CONNECT_ERROR, e);
        }
        return hConnection;
    }

    public static Table getTable(Configuration configuration)
    {
        String hbaseConfig = configuration.getString(HBaseKey.HBASE_CONFIG);
        String userTable = configuration.getString(HBaseKey.TABLE);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        org.apache.hadoop.hbase.client.Table hTable = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin, hTableName);
            hTable = hConnection.getTable(hTableName);
        }
        catch (Exception e) {
            Hbase11xHelper.closeTable(hTable);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        return hTable;
    }

    public static RegionLocator getRegionLocator(Configuration configuration)
    {
        String hbaseConfig = configuration.getString(HBaseKey.HBASE_CONFIG);
        String userTable = configuration.getString(HBaseKey.TABLE);
        org.apache.hadoop.hbase.client.Connection hConnection = Hbase11xHelper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        RegionLocator regionLocator = null;
        try {
            admin = hConnection.getAdmin();
            Hbase11xHelper.checkHbaseTable(admin, hTableName);
            regionLocator = hConnection.getRegionLocator(hTableName);
        }
        catch (Exception e) {
            Hbase11xHelper.closeRegionLocator(regionLocator);
            Hbase11xHelper.closeAdmin(admin);
            Hbase11xHelper.closeConnection(hConnection);
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        return regionLocator;
    }

    public static synchronized void closeConnection(Connection hConnection)
    {
        try {
            if (null != hConnection) {
                hConnection.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
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
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
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
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    public static void closeResultScanner(ResultScanner resultScanner)
    {
        if (null != resultScanner) {
            resultScanner.close();
        }
    }

    public static void closeRegionLocator(RegionLocator regionLocator)
    {
        try {
            if (null != regionLocator) {
                regionLocator.close();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    public static void checkHbaseTable(Admin admin, TableName hTableName)
            throws IOException
    {
        if (!admin.tableExists(hTableName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The table " + hTableName.toString() + " does not exists.");
        }
        if (!admin.isTableAvailable(hTableName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The table " + hTableName.toString() + " is unavailable");
        }
        if (admin.isTableDisabled(hTableName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The table " + hTableName.toString() + "is disabled");
        }
    }

    public static byte[] convertUserStartRowkey(Configuration configuration)
    {
        String startRowkey = configuration.getString(HBaseKey.START_ROW_KEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }
        else {
            boolean isBinaryRowkey = configuration.getBool(HBaseKey.IS_BINARY_ROW_KEY);
            return Hbase11xHelper.stringToBytes(startRowkey, isBinaryRowkey);
        }
    }

    public static byte[] convertUserEndRowkey(Configuration configuration)
    {
        String endRowkey = configuration.getString(HBaseKey.END_ROW_KEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }
        else {
            boolean isBinaryRowkey = configuration.getBool(HBaseKey.IS_BINARY_ROW_KEY);
            return Hbase11xHelper.stringToBytes(endRowkey, isBinaryRowkey);
        }
    }

    /**
     * note: convertUserStartRowkey and convertInnerStartRowkey, the former will be affected by isBinaryRowkey,
     * only used for the first time to convert the user-configured String type rowkey to binary. The latter is
     * agreed: when the binary rowkey obtained during the split is filled back to the configuration, it is used
     * @param configuration Configuration
     * @return byte[]
     */
    public static byte[] convertInnerStartRowkey(Configuration configuration)
    {
        String startRowkey = configuration.getString(HBaseKey.START_ROW_KEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(startRowkey);
    }

    public static byte[] convertInnerEndRowkey(Configuration configuration)
    {
        String endRowkey = configuration.getString(HBaseKey.END_ROW_KEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(endRowkey);
    }

    private static byte[] stringToBytes(String rowkey, boolean isBinaryRowkey)
    {
        if (isBinaryRowkey) {
            return Bytes.toBytesBinary(rowkey);
        }
        else {
            return Bytes.toBytes(rowkey);
        }
    }

    public static boolean isRowkeyColumn(String columnName)
    {
        return HBaseConstant.ROWKEY_FLAG.equalsIgnoreCase(columnName);
    }


    public static List<HbaseColumnCell> parseColumnOfNormalMode(List<Map> column)
    {
        List<HbaseColumnCell> hbaseColumnCells = new ArrayList<>();

        HbaseColumnCell oneColumnCell;

        for (Map<String, String> aColumn : column) {
            ColumnType type = ColumnType.getByTypeName(aColumn.get(HBaseKey.TYPE));
            String columnName = aColumn.get(HBaseKey.NAME);
            String columnValue = aColumn.get(HBaseKey.VALUE);
            String dateformat = aColumn.get(HBaseKey.FORMAT);

            if (type == ColumnType.DATE) {

                if (dateformat == null) {
                    dateformat = HBaseConstant.DEFAULT_DATE_FORMAT;
                }
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "invalid configuration for " + aColumn);

                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .dateFormat(dateformat)
                        .build();
            }
            else {
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "invalid configuration for " + aColumn);
                oneColumnCell = new HbaseColumnCell.Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .build();
            }

            hbaseColumnCells.add(oneColumnCell);
        }

        return hbaseColumnCells;
    }

    //将多竖表column变成<familyQualifier,<>>形式
    public static HashMap<String, HashMap<String, String>> parseColumnOfMultiVersionMode(List<Map> column)
    {

        HashMap<String, HashMap<String, String>> familyQualifierMap = new HashMap<>();
        for (Map<String, String> aColumn : column) {
            String type = aColumn.get(HBaseKey.TYPE);
            String columnName = aColumn.get(HBaseKey.NAME);
            String dateformat = aColumn.get(HBaseKey.FORMAT);

            ColumnType.getByTypeName(type);
            Validate.isTrue(StringUtils.isNotBlank(columnName), "The name inf columns must be form with cf:qualifier");

            String familyQualifier;
            if (!Hbase11xHelper.isRowkeyColumn(columnName)) {
                String[] cfAndQualifier = columnName.split(":");
                if (cfAndQualifier.length != 2) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The name inf columns must be form with cf:qualifier");
                }
                familyQualifier = StringUtils.join(cfAndQualifier[0].trim(), ":", cfAndQualifier[1].trim());
            }
            else {
                familyQualifier = columnName.trim();
            }

            HashMap<String, String> typeAndFormat = new HashMap<>();
            typeAndFormat.put(HBaseKey.TYPE, type);
            typeAndFormat.put(HBaseKey.FORMAT, dateformat);
            familyQualifierMap.put(familyQualifier, typeAndFormat);
        }
        return familyQualifierMap;
    }

    public static List<Configuration> split(Configuration configuration)
    {
        byte[] startRowkeyByte = Hbase11xHelper.convertUserStartRowkey(configuration);
        byte[] endRowkeyByte = Hbase11xHelper.convertUserEndRowkey(configuration);

        // if  user has configured startRowkey and endRowkey, make sure that startRowkey <= endRowkey
        if (startRowkeyByte.length != 0 && endRowkeyByte.length != 0
                && Bytes.compareTo(startRowkeyByte, endRowkeyByte) > 0) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, "the number of startRowkey must less than endRowkey.");
        }
        RegionLocator regionLocator = Hbase11xHelper.getRegionLocator(configuration);
        List<Configuration> resultConfigurations;
        try {
            Pair<byte[][], byte[][]> regionRanges = regionLocator.getStartEndKeys();
            if (null == regionRanges) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, "failed to get the range of rowkey");
            }
            resultConfigurations = Hbase11xHelper.doSplit(configuration, startRowkeyByte, endRowkeyByte,
                    regionRanges);

            LOG.info("HBaseReader split job into {} tasks.", resultConfigurations.size());
            return resultConfigurations;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL, "failed to split table", e);
        }
        finally {
            Hbase11xHelper.closeRegionLocator(regionLocator);
        }
    }

    private static List<Configuration> doSplit(Configuration config, byte[] startRowkeyByte,
            byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges)
    {

        List<Configuration> configurations = new ArrayList<>();

        for (int i = 0; i < regionRanges.getFirst().length; i++) {

            byte[] regionStartKey = regionRanges.getFirst()[i];
            byte[] regionEndKey = regionRanges.getSecond()[i];

            if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0
                    && (endRowkeyByte.length != 0 && (Bytes.compareTo(
                    regionStartKey, endRowkeyByte) > 0))) {
                continue;
            }

            if ((Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0)
                    && (Bytes.compareTo(startRowkeyByte, regionEndKey) >= 0)) {
                continue;
            }

            if (endRowkeyByte.length != 0
                    && (Bytes.compareTo(endRowkeyByte, regionStartKey) <= 0)) {
                continue;
            }

            Configuration p = config.clone();

            String thisStartKey = getStartKey(startRowkeyByte, regionStartKey);

            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey);

            p.set(HBaseKey.START_ROW_KEY, thisStartKey);
            p.set(HBaseKey.END_ROW_KEY, thisEndKey);

            LOG.debug("startRowkey:[{}], endRowkey:[{}] .", thisStartKey, thisEndKey);

            configurations.add(p);
        }

        return configurations;
    }

    private static String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey)
    {
        if (endRowkeyByte == null) {
            throw new IllegalArgumentException("userEndKey should not be null!");
        }

        byte[] tempEndRowkeyByte;

        if (endRowkeyByte.length == 0) {
            tempEndRowkeyByte = regionEndKey;
        }
        else if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
            tempEndRowkeyByte = endRowkeyByte;
        }
        else {
            if (Bytes.compareTo(endRowkeyByte, regionEndKey) > 0) {
                tempEndRowkeyByte = regionEndKey;
            }
            else {
                tempEndRowkeyByte = endRowkeyByte;
            }
        }

        return Bytes.toStringBinary(tempEndRowkeyByte);
    }

    private static String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey)
    {
        if (startRowkeyByte == null) {
            throw new IllegalArgumentException(
                    "userStartKey should not be null!");
        }

        byte[] tempStartRowkeyByte;

        if (Bytes.compareTo(startRowkeyByte, regionStarKey) < 0) {
            tempStartRowkeyByte = regionStarKey;
        }
        else {
            tempStartRowkeyByte = startRowkeyByte;
        }
        return Bytes.toStringBinary(tempStartRowkeyByte);
    }

    public static void validateParameter(Configuration originalConfig)
    {
        originalConfig.getNecessaryValue(HBaseKey.HBASE_CONFIG, REQUIRED_VALUE);
        originalConfig.getNecessaryValue(HBaseKey.TABLE, REQUIRED_VALUE);

        Hbase11xHelper.validateMode(originalConfig);

        String encoding = originalConfig.getString(HBaseKey.ENCODING, HBaseConstant.DEFAULT_ENCODING);
        if (!Charset.isSupported(encoding)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, String.format("The encoding '" + encoding + "' is not supported."));
        }
        originalConfig.set(HBaseKey.ENCODING, encoding);
        String startRowkey = originalConfig.getString(HBaseConstant.RANGE + "." + HBaseKey.START_ROW_KEY);

        if (startRowkey != null && !startRowkey.isEmpty()) {
            originalConfig.set(HBaseKey.START_ROW_KEY, startRowkey);
        }

        String endRowkey = originalConfig.getString(HBaseConstant.RANGE + "." + HBaseKey.END_ROW_KEY);
        if (endRowkey != null && !endRowkey.isEmpty()) {
            originalConfig.set(HBaseKey.END_ROW_KEY, endRowkey);
        }
        Boolean isBinaryRowkey = originalConfig.getBool(HBaseConstant.RANGE + "." + HBaseKey.IS_BINARY_ROW_KEY, false);
        originalConfig.set(HBaseKey.IS_BINARY_ROW_KEY, isBinaryRowkey);

        //scan cache
        int scanCacheSize = originalConfig.getInt(HBaseKey.SCAN_CACHE_SIZE, HBaseConstant.DEFAULT_SCAN_CACHE_SIZE);
        originalConfig.set(HBaseKey.SCAN_CACHE_SIZE, scanCacheSize);

        int scanBatchSize = originalConfig.getInt(HBaseKey.SCAN_BATCH_SIZE, HBaseConstant.DEFAULT_SCAN_BATCH_SIZE);
        originalConfig.set(HBaseKey.SCAN_BATCH_SIZE, scanBatchSize);
    }

    private static void validateMode(Configuration originalConfig)
    {
        String mode = originalConfig.getNecessaryValue(HBaseKey.MODE, REQUIRED_VALUE);
        List<Map> column = originalConfig.getList(HBaseKey.COLUMN, Map.class);
        if (column == null || column.isEmpty()) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE, "The configuration item column is required");
        }
        ModeType modeType = ModeType.getByTypeName(mode);
        switch (modeType) {
            case NORMAL: {
                String maxVersion = originalConfig.getString(HBaseKey.MAX_VERSION);
                Validate.isTrue(maxVersion == null, "The configuration item maxVersion is not allowed in normal mode");
                Hbase11xHelper.parseColumnOfNormalMode(column);
                break;
            }
            case MULTI_VERSION_FIXED_COLUMN: {
                checkMaxVersion(originalConfig, mode);

                Hbase11xHelper.parseColumnOfMultiVersionMode(column);
                break;
            }
            default:
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The mode type '" + mode + "' is not supported");
        }
    }

    private static void checkMaxVersion(Configuration configuration, String mode)
    {
        Integer maxVersion = configuration.getInt(HBaseKey.MAX_VERSION);
        Validate.notNull(maxVersion, "The configuration item maxVersion is required in " + mode + " mode");
        boolean isMaxVersionValid = maxVersion == -1 || maxVersion > 1;
        Validate.isTrue(isMaxVersionValid,"The maxVersion value is illegal, it either is -1 or greater than 1");
    }
}
