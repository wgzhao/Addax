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

package com.wgzhao.addax.plugin.writer.elasticsearchwriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.util.RetryUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class ESWriter
        extends Writer
{
    private static final String WRITE_COLUMNS = "write_columns";

    public static class Job
            extends Writer.Job
    {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init()
        {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public void prepare()
        {
            ESClient esClient = new ESClient();
            esClient.createClient(ESKey.getEndpoint(conf),
                    ESKey.getAccessID(conf),
                    ESKey.getAccessKey(conf),
                    ESKey.isMultiThread(conf),
                    ESKey.getTimeout(conf),
                    ESKey.isCompression(conf),
                    ESKey.isDiscovery(conf));

            String indexName = ESKey.getIndexName(conf);
            String typeName = ESKey.getTypeName(conf);
            boolean dynamic = ESKey.getDynamic(conf);
            String mappings = genMappings(typeName);
            String settings = JSON.toJSONString(
                    ESKey.getSettings(conf)
            );
            log.info("index:[{}], type:[{}], mappings:[{}]", indexName, typeName, mappings);

            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (ESKey.isCleanup(this.conf) && isIndicesExists) {
                    esClient.deleteIndex(indexName);
                }
                if (!esClient.createIndex(indexName, typeName, mappings, settings, dynamic)) {
                    throw new IOException("create index or mapping failed");
                }
            }
            catch (Exception ex) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, ex.toString());
            }
            esClient.closeJestClient();
        }

        private String genMappings(String typeName)
        {
            String mappings;
            Map<String, Object> propMap = new HashMap<>();
            List<ESColumn> columnList = new ArrayList<>();

            List column = conf.getList("column");
            if (column != null) {
                for (Object col : column) {
                    JSONObject jo = JSON.parseObject(col.toString());
                    String colName = jo.getString("name");
                    String colTypeStr = jo.getString("type");
                    if (colTypeStr == null) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR, col + " column must have type");
                    }
                    ESFieldType colType = ESFieldType.getESFieldType(colTypeStr);
                    if (colType == null) {
                        throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, col + " unsupported type");
                    }

                    ESColumn columnItem = new ESColumn();

                    if (colName.equals(ESKey.PRIMARY_KEY_COLUMN_NAME)) {
                        // compatible with old addax version
                        colType = ESFieldType.ID;
                        colTypeStr = "id";
                    }

                    columnItem.setName(colName);
                    columnItem.setType(colTypeStr);

                    if (colType == ESFieldType.ID) {
                        columnList.add(columnItem);
                        continue;
                    }

                    Boolean array = jo.getBoolean("array");
                    if (array != null) {
                        columnItem.setArray(array);
                    }
                    Map<String, Object> field = new HashMap<>();
                    field.put("type", colTypeStr);
                    //https://www.elastic.co/guide/en/elasticsearch/reference/5.2/breaking_50_mapping_changes.html#_literal_index_literal_property
                    // https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_deep_dive_on_doc_values.html#_disabling_doc_values
                    field.put("doc_values", jo.getBoolean("doc_values"));
                    field.put("ignore_above", jo.getInteger("ignore_above"));
                    field.put("index", jo.getBoolean("index"));

                    switch (colType) {
                        case STRING:
                            // compatible with es version 5 or before
                            break;
                        case KEYWORD:
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#_warm_up_global_ordinals
                            field.put("eager_global_ordinals", jo.getBoolean("eager_global_ordinals"));
                            break;
                        case TEXT:
                            field.put("analyzer", jo.getString("analyzer"));
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-disk-usage.html
                            field.put("norms", jo.getBoolean("norms"));
                            field.put("index_options", jo.getBoolean("index_options"));
                            break;
                        case DATE:
                            columnItem.setTimeZone(jo.getString("timezone"));
                            columnItem.setFormat(jo.getString("format"));
                            break;
                        case GEO_SHAPE:
                            field.put("tree", jo.getString("tree"));
                            field.put("precision", jo.getString("precision"));
                            break;
                        default:
                            break;
                    }
                    propMap.put(colName, field);
                    columnList.add(columnItem);
                }
            }

            conf.set(WRITE_COLUMNS, JSON.toJSONString(columnList));

            log.info(JSON.toJSONString(columnList));

            Map<String, Object> rootMappings = new HashMap<>();
            Map<String, Object> typeMappings = new HashMap<>();
            typeMappings.put("properties", propMap);
            rootMappings.put(typeName, typeMappings);

            mappings = JSON.toJSONString(rootMappings);

            if (mappings == null || mappings.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "must have mappings");
            }

            return mappings;
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void post()
        {
            ESClient esClient = new ESClient();
            esClient.createClient(ESKey.getEndpoint(conf),
                    ESKey.getAccessID(conf),
                    ESKey.getAccessKey(conf),
                    false,
                    300000,
                    false,
                    false);
            String alias = ESKey.getAlias(conf);
            if (!"".equals(alias)) {
                log.info(String.format("alias [%s] to [%s]", alias, ESKey.getIndexName(conf)));
                try {
                    esClient.alias(ESKey.getIndexName(conf), alias, ESKey.isNeedCleanAlias(conf));
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
                }
            }
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Writer.Task
    {

        private static final Logger log = LoggerFactory.getLogger(Task.class);
        ESClient esClient = null;
        private Configuration conf;
        private List<ESFieldType> typeList;
        private List<ESColumn> columnList;

        private int trySize;
        private int batchSize;
        private String index;
        private String type;
        private String splitter;

        @Override
        public void init()
        {
            this.conf = super.getPluginJobConf();
            index = ESKey.getIndexName(conf);
            type = ESKey.getTypeName(conf);

            trySize = ESKey.getTrySize(conf);
            batchSize = ESKey.getBatchSize(conf);
            splitter = ESKey.getSplitter(conf);
            columnList = JSON.parseObject(this.conf.getString(WRITE_COLUMNS), new TypeReference<List<ESColumn>>()
            {
            });

            typeList = new ArrayList<>();

            for (ESColumn col : columnList) {
                typeList.add(ESFieldType.getESFieldType(col.getType()));
            }

            esClient = new ESClient();
        }

        @Override
        public void prepare()
        {
            esClient.createClient(ESKey.getEndpoint(conf),
                    ESKey.getAccessID(conf),
                    ESKey.getAccessKey(conf),
                    ESKey.isMultiThread(conf),
                    ESKey.getTimeout(conf),
                    ESKey.isCompression(conf),
                    ESKey.isDiscovery(conf));
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver)
        {
            List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            Record record;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }

            if (!writerBuffer.isEmpty()) {
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }

            String msg = String.format("task end, write size :%d", total);
            getTaskPluginCollector().collectMessage("writeSize", String.valueOf(total));
            log.info(msg);
            esClient.closeJestClient();
        }

        private String getDateStr(ESColumn esColumn, Column column)
        {
            DateTime date;
            DateTimeZone dtz = DateTimeZone.getDefault();
            if (esColumn.getTimezone() != null) {
                // http://www.joda.org/joda-time/timezones.html
                dtz = DateTimeZone.forID(esColumn.getTimezone());
            }
            if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
                date = formatter.withZone(dtz).parseDateTime(column.asString());
                return date.toString();
            }
            else if (column.getType() == Column.Type.DATE) {
                date = new DateTime(column.asLong(), dtz);
                return date.toString();
            }
            else {
                return column.asString();
            }
        }

        private long doBatchInsert(final List<Record> writerBuffer)
        {
            Map<String, Object> data;
            final Bulk.Builder bulkAction = new Bulk.Builder().defaultIndex(this.index).defaultType(this.type);
            for (Record record : writerBuffer) {
                data = new HashMap<>();
                StringBuilder id = new StringBuilder();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    String columnName = columnList.get(i).getName();
                    ESFieldType columnType = typeList.get(i);
                    // for array type, it must be string type
                    if (columnList.get(i).isArray() != null && columnList.get(i).isArray()) {
                        if (null == column.asString()) {
                            data.put(columnName, null);
                        } else {
                            String[] dataList = column.asString().split(splitter);
                            if (!columnType.equals(ESFieldType.DATE)) {
                                data.put(columnName, dataList);
                            }
                            else {
                                for (int pos = 0; pos < dataList.length; pos++) {
                                    dataList[pos] = getDateStr(columnList.get(i), column);
                                }
                                data.put(columnName, dataList);
                            }
                        }
                    }
                    else {
                        switch (columnType) {
                            case ID:
                                id.append(record.getColumn(i).asString());
                                break;
                            case DATE:
                                try {
                                    String dateStr = getDateStr(columnList.get(i), column);
                                    data.put(columnName, dateStr);
                                }
                                catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("failed to parse " +  columnName + " : "  + e));
                                }
                                break;
                            case KEYWORD:
                            case STRING:
                            case TEXT:
                            case IP:
                            case GEO_POINT:
                                data.put(columnName, column.asString());
                                break;
                            case BOOLEAN:
                                data.put(columnName, column.asBoolean());
                                break;
                            case BYTE:
                            case BINARY:
                                data.put(columnName, column.asBytes());
                                break;
                            case LONG:
                                data.put(columnName, column.asLong());
                                break;
                            case INTEGER:
                            case SHORT:
                                data.put(columnName, column.asBigInteger());
                                break;
                            case FLOAT:
                            case DOUBLE:
                                data.put(columnName, column.asDouble());
                                break;
                            case NESTED:
                            case OBJECT:
                            case FLATTENED:
                            case GEO_SHAPE:
                                data.put(columnName, JSON.parse(column.asString()));
                                break;
                            default:
                                getTaskPluginCollector().collectDirtyRecord(record, "The column type " + columnType + " is not supported ");
                        }
                    }
                }

                if (id.capacity() == 0) {
                    //id = UUID.randomUUID().toString()
                    bulkAction.addAction(new Index.Builder(data).build());
                }
                else {
                    bulkAction.addAction(new Index.Builder(data).id(id.toString()).build());
                }
            }

            try {
                return RetryUtil.executeWithRetry(new Callable<Integer>()
                {
                    @Override
                    public Integer call()
                            throws Exception
                    {
                        JestResult jestResult = esClient.bulkInsert(bulkAction, 1);
                        if (jestResult.isSucceeded()) {
                            return writerBuffer.size();
                        }

                        String msg = String.format("response code: [%d] error :[%s]", jestResult.getResponseCode(), jestResult.getErrorMessage());
                        log.warn(msg);
                        if (esClient.isBulkResult(jestResult)) {
                            BulkResult brst = (BulkResult) jestResult;
                            List<BulkResult.BulkResultItem> failedItems = brst.getFailedItems();
                            for (BulkResult.BulkResultItem item : failedItems) {
                                if (item.status != 400) {
                                    // 400 BAD_REQUEST  如果非数据异常,请求异常,则不允许忽略
                                    throw AddaxException.asAddaxException(EXECUTE_FAIL, String.format("status:[%d], error: %s", item.status, item.error));
                                }
                                else {
                                    // 如果用户选择不忽略解析错误,则抛异常,默认为忽略
                                    if (!ESKey.isIgnoreParseError(conf)) {
                                        throw AddaxException.asAddaxException(EXECUTE_FAIL, String.format("status:[%d], error: %s, config not ignoreParseError so throw this error", item.status, item.error));
                                    }
                                }
                            }

                            List<BulkResult.BulkResultItem> items = brst.getItems();
                            for (int idx = 0; idx < items.size(); ++idx) {
                                BulkResult.BulkResultItem item = items.get(idx);
                                if (item.error != null && !"".equals(item.error)) {
                                    getTaskPluginCollector().collectDirtyRecord(writerBuffer.get(idx), String.format("status:[%d], error: %s", item.status, item.error));
                                }
                            }
                            return writerBuffer.size() - brst.getFailedItems().size();
                        }
                        else {
                            Integer status = esClient.getStatus(jestResult);
                            if (status == 429) {
                                //TOO_MANY_REQUESTS
                                log.warn("server response too many requests, so auto reduce speed");
                            }
                            throw AddaxException.asAddaxException(EXECUTE_FAIL, jestResult.getErrorMessage());
                        }
                    }
                }, trySize, 60000L, true);
            }
            catch (Exception e) {
                if (ESKey.isIgnoreWriteError(this.conf)) {
                    log.warn("failed to write in " +  trySize + " times, so ignore it");
                }
                else {
                    throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
                }
            }
            return 0;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            esClient.closeJestClient();
        }
    }
}
