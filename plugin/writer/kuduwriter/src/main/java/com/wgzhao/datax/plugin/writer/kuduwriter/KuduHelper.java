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

package com.wgzhao.datax.plugin.writer.kuduwriter;

import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KuduHelper
{

    private static final Logger LOG = LoggerFactory.getLogger(KuduHelper.class);

    public static KuduClient getKuduClient(Configuration configuration)
    {
        try {
            String masterAddress = (String) configuration.get(Key.KUDU_MASTER_ADDRESSES);
            return new KuduClient.KuduClientBuilder(masterAddress)
                    .defaultAdminOperationTimeoutMs(
                            Long.parseLong(configuration.getString(
                                    Key.KUDU_ADMIN_TIMEOUT, "60")) * 1000L)
                    .defaultOperationTimeoutMs(
                            Long.parseLong(configuration.getString(
                                    Key.KUDU_SESSION_TIMEOUT, "100"))* 1000L)
                    .build();
        }
        catch (Exception e) {
            throw DataXException.asDataXException(KuduWriterErrorCode.GET_KUDU_CONNECTION_ERROR, e);
        }
    }

    public static KuduTable getKuduTable(KuduClient kuduClient, String tableName)
    {
        try {
            return kuduClient.openTable(tableName);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(KuduWriterErrorCode.GET_KUDU_TABLE_ERROR, e);
        }
    }

    public static ThreadPoolExecutor createRowAddThreadPool(int coreSize)
    {
        return new ThreadPoolExecutor(coreSize,
                coreSize,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactory()
                {
                    private final ThreadGroup group = System.getSecurityManager() == null ?
                            Thread.currentThread().getThreadGroup() : System.getSecurityManager().getThreadGroup();
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r)
                    {
                        Thread t = new Thread(group, r,
                                "pool-kudu_rows_add-thread-" + threadNumber.getAndIncrement(),
                                0);
                        if (t.isDaemon()) {
                            t.setDaemon(false);
                        }
                        if (t.getPriority() != Thread.NORM_PRIORITY) {
                            t.setPriority(Thread.NORM_PRIORITY);
                        }
                        return t;
                    }
                }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static List<List<Configuration>> getColumnLists(List<Configuration> columns)
    {
        int quota = 8;
        int num = (columns.size() - 1) / quota + 1;
        int gap = columns.size() / num;
        List<List<Configuration>> columnLists = new ArrayList<>(num);
        for (int j = 0; j < num - 1; j++) {
            List<Configuration> destList = new ArrayList<>(columns.subList(j * gap, (j + 1) * gap));
            columnLists.add(destList);
        }
        List<Configuration> destList = new ArrayList<>(columns.subList(gap * (num - 1), columns.size()));
        columnLists.add(destList);
        return columnLists;
    }

    public static boolean isTableExists(Configuration configuration)
    {
        String tableName = configuration.getString(Key.KUDU_TABLE_NAME);
        String kuduConfig = configuration.getString(Key.KUDU_CONFIG);
        KuduClient kuduClient =KuduHelper.getKuduClient(configuration);
        try {
            return kuduClient.tableExists(tableName);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(KuduWriterErrorCode.GET_KUDU_CONNECTION_ERROR, e);
        }
        finally {
            KuduHelper.closeClient(kuduClient);
        }
    }

    public static void closeClient(KuduClient kuduClient)
    {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        }
        catch (KuduException e) {
            LOG.warn("The \"kudu client\" was not stopped gracefully. !");
        }
    }

    public static Schema getSchema(Configuration configuration)
    {
        List<Configuration> columns = configuration.getListConfiguration(Key.COLUMN);
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        Schema schema = null;
        if (columns == null || columns.isEmpty()) {
            throw DataXException.asDataXException(KuduWriterErrorCode.REQUIRED_VALUE,
                    "column is not defined，eg：column:[{\"name\": \"cf0:column0\",\"type\": \"string\"},{\"name\": \"cf1:column1\",\"type\": \"long\"}]");
        }
        try {
            for (Configuration column : columns) {

                String type = "BIGINT".equalsIgnoreCase(column.getNecessaryValue(Key.TYPE, KuduWriterErrorCode.REQUIRED_VALUE)) ||
                        "LONG".equalsIgnoreCase(column.getNecessaryValue(Key.TYPE, KuduWriterErrorCode.REQUIRED_VALUE)) ?
                        "INT64" : "INT".equalsIgnoreCase(column.getNecessaryValue(Key.TYPE, KuduWriterErrorCode.REQUIRED_VALUE)) ?
                        "INT32" : column.getNecessaryValue(Key.TYPE, KuduWriterErrorCode.REQUIRED_VALUE).toUpperCase();
                String name = column.getNecessaryValue(Key.NAME, KuduWriterErrorCode.REQUIRED_VALUE);
                Boolean key = column.getBool(Key.PRIMARYKEY, false);
                String encoding = column.getString(Key.ENCODING, Constant.ENCODING).toUpperCase();
                String compression = column.getString(Key.COMPRESSION, Constant.COMPRESSION).toUpperCase();
                String comment = column.getString(Key.COMMENT, "");

                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(name, Type.getTypeForName(type))
                        .key(key)
                        .encoding(ColumnSchema.Encoding.valueOf(encoding))
                        .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.valueOf(compression))
                        .comment(comment)
                        .build());
            }
            schema = new Schema(columnSchemas);
        }
        catch (Exception e) {
            throw DataXException.asDataXException(KuduWriterErrorCode.REQUIRED_VALUE, e);
        }
        return schema;
    }

    public static Integer getPrimaryKeyIndexUntil(List<Configuration> columns)
    {
        int i = 0;
        while (i < columns.size()) {
            Configuration col = columns.get(i);
            if (!col.getBool(Key.PRIMARYKEY, false)) {
                break;
            }
            i++;
        }
        return i;
    }

    public static void setTablePartition(Configuration configuration,
            CreateTableOptions tableOptions,
            Schema schema)
    {
        Configuration partition = configuration.getConfiguration(Key.PARTITION);
        if (partition == null) {
            ColumnSchema columnSchema = schema.getColumns().get(0);
            tableOptions.addHashPartitions(Collections.singletonList(columnSchema.getName()), 3);
            return;
        }
        //range分区
        Map<String, Object> range = partition.getMap(Key.RANGE);
        if (range != null && !range.isEmpty()) {
            Set<Map.Entry<String, Object>> rangeColums = range.entrySet();
            for (Map.Entry<String, Object> rangeColum : rangeColums) {
                JSONArray lowerAndUppers = (JSONArray) rangeColum.getValue();
                Iterator<Object> iterator = lowerAndUppers.iterator();
                String colum = rangeColum.getKey();
                if (StringUtils.isBlank(colum)) {
                    throw DataXException.asDataXException(KuduWriterErrorCode.REQUIRED_VALUE,
                            "range partition column is empty, please check the configuration parameters.");
                }
                while (iterator.hasNext()) {
                    JSONObject lowerAndUpper = (JSONObject) iterator.next();
                    String lowerValue = lowerAndUpper.getString(Key.LOWER);
                    String upperValue = lowerAndUpper.getString(Key.UPPER);
                    if (StringUtils.isBlank(lowerValue) || StringUtils.isBlank(upperValue)) {
                        throw DataXException.asDataXException(KuduWriterErrorCode.REQUIRED_VALUE,
                                "\"lower\" or \"upper\" is empty, please check the configuration parameters.");
                    }
                    PartialRow lower = schema.newPartialRow();
                    PartialRow upper = schema.newPartialRow();
                    lower.addString(colum, lowerValue);
                    upper.addString(colum, upperValue);
                    tableOptions.addRangePartition(lower, upper);
                }
            }
            LOG.info("Set range partition complete!");
        }

        // 设置Hash分区
        Configuration hash = partition.getConfiguration(Key.HASH);
        if (hash != null) {
            List<String> hashColums = hash.getList(Key.COLUMN, String.class);
            Integer hashPartitionNum = configuration.getInt(Key.HASH_NUM, 3);
            tableOptions.addHashPartitions(hashColums, hashPartitionNum);
            LOG.info("Set hash partition complete!");
        }
    }

    public static void validateParameter(Configuration configuration)
    {
        LOG.info("Start validating parameters！");
        // configuration.getNecessaryValue(Key.KUDU_CONFIG, KuduWriterErrorCode.REQUIRED_VALUE);
        configuration.getNecessaryValue(Key.KUDU_TABLE_NAME, KuduWriterErrorCode.REQUIRED_VALUE);
        configuration.getNecessaryValue(Key.KUDU_MASTER_ADDRESSES, KuduWriterErrorCode.REQUIRED_VALUE);
//        String encoding = configuration.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
//        if (!Charset.isSupported(encoding)) {
//            throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
//                    String.format("Encoding is not supported:[%s] .", encoding));
//        }
//        configuration.set(Key.ENCODING, encoding);
        String insertMode = configuration.getString(Key.WRITE_MODE, Constant.INSERT_MODE);
        try {
            InsertModeType.getByTypeName(insertMode);
        }
        catch (Exception e) {
            insertMode = Constant.INSERT_MODE;
        }
        configuration.set(Key.WRITE_MODE, insertMode);

        Long writeBufferSize = configuration.getLong(Key.WRITE_BATCH_SIZE, Constant.DEFAULT_WRITE_BATCH_SIZE);
        configuration.set(Key.WRITE_BATCH_SIZE, writeBufferSize);

        Long mutationBufferSpace = configuration.getLong(Key.MUTATION_BUFFER_SPACE, Constant.DEFAULT_MUTATION_BUFFER_SPACE);
        configuration.set(Key.MUTATION_BUFFER_SPACE, mutationBufferSpace);

        Boolean isSkipFail = configuration.getBool(Key.SKIP_FAIL, false);
        configuration.set(Key.SKIP_FAIL, isSkipFail);
        List<Configuration> columns = configuration.getListConfiguration(Key.COLUMN);
        List<Configuration> goalColumns = new ArrayList<>();
        //column参数验证
        int indexFlag = 0;
        boolean primaryKey = true;
        int primaryKeyFlag = 0;
        for (int i = 0; i < columns.size(); i++) {
            Configuration col = columns.get(i);
            String index = col.getString(Key.INDEX);
            if (index == null) {
                index = String.valueOf(i);
                col.set(Key.INDEX, index);
                indexFlag++;
            }
//            if (primaryKey != col.getBool(Key.PRIMARYKEY, false)) {
//                primaryKey = col.getBool(Key.PRIMARYKEY, false);
//                primaryKeyFlag++;
//            }
            goalColumns.add(col);
        }
        if (indexFlag != 0 && indexFlag != columns.size()) {
            throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
                    "\"index\" either has values for all of them, or all of them are null!");
        }
//        if (primaryKeyFlag > 1) {
//            throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
//                    "\"primaryKey\" must be written in the front！");
//        }
        configuration.set(Key.COLUMN, goalColumns);
//        LOG.info("------------------------------------");
//        LOG.info(configuration.toString());
//        LOG.info("------------------------------------");
        LOG.info("validate parameter complete！");
    }

    public static void truncateTable(Configuration configuration)
    {
        String kuduConfig = configuration.getString(Key.KUDU_CONFIG);
        String userTable = configuration.getString(Key.KUDU_TABLE_NAME);
        LOG.info(String.format("Because you have configured truncate is true,KuduWriter begins to truncate table %s .", userTable));
        KuduClient kuduClient = KuduHelper.getKuduClient(configuration);

        try {
            if (kuduClient.tableExists(userTable)) {
                kuduClient.deleteTable(userTable);
                LOG.info(String.format("table  %s has been deleted.", userTable));
            }
        }
        catch (KuduException e) {
            throw DataXException.asDataXException(KuduWriterErrorCode.DELETE_KUDU_ERROR, e);
        }
        finally {
            KuduHelper.closeClient(kuduClient);
        }
    }

    public static List<String> getColumnNames(List<Configuration> columns)
{
    List<String> columnNames = Lists.newArrayList();
    for (Configuration eachColumnConf : columns) {
        columnNames.add(eachColumnConf.getString(Key.NAME));
    }
    return columnNames;
}

}
