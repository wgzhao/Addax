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

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.RetryUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** ESWriter. */
public class ESWriter
        extends Writer
{
    private static final String WRITE_COLUMNS = "write_columns";

    /** The shape joda-time used to render, and the one elasticsearch's default parser reads. */
    private static final DateTimeFormatter ISO_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    /** Job. */
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
            boolean dynamic = ESKey.getDynamic(conf);
            String mappings = genMappings();
            String settings = JSON.toJSONString(
                    ESKey.getSettings(conf)
            );
            log.info("index:[{}], mappings:[{}]", indexName, mappings);

            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (ESKey.isCleanup(this.conf) && isIndicesExists) {
                    esClient.deleteIndex(indexName);
                }
                if (!esClient.createIndex(indexName, mappings, settings, dynamic)) {
                    throw new IOException("create index or mapping failed");
                }
            }
            catch (Exception ex) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, ex.toString());
            }
            esClient.closeJestClient();
        }

        /**
         * Builds the mappings of the index from the column configuration.
         *
         * <p>Elasticsearch 7.0 removed the mapping type, so this is a plain
         * {@code {"properties": {...}}} body, for the create request as well as for the one
         * that puts the mappings of an index that is already there.
         */
        private String genMappings()
        {
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
                            field.put("index_options", jo.getString("index_options"));
                            break;
                        case DATE:
                            columnItem.setTimezone(jo.getString("timezone"));
                            columnItem.setFormat(jo.getString("format"));
                            break;
                        case GEO_SHAPE:
                            if (jo.containsKey("tree") || jo.containsKey("precision")) {
                                // both were part of the quadtree implementation that
                                // elasticsearch 6 replaced with a BKD tree, and the mapping is
                                // rejected when they are still configured
                                log.warn("column[{}]: tree and precision are not supported by elasticsearch 6 and later, ignoring them", colName);
                            }
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

            Map<String, Object> mappings = new HashMap<>();
            mappings.put("properties", propMap);

            return JSON.toJSONString(mappings);
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

    /** Task. */
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
        private int parallelBulk;
        private String index;
        private String splitter;
        private boolean recordShapeChecked;
        private boolean missingIdLogged;

        @Override
        public void init()
        {
            this.conf = super.getPluginJobConf();
            index = ESKey.getIndexName(conf);

            trySize = ESKey.getTrySize(conf);
            batchSize = ESKey.getBatchSize(conf);
            parallelBulk = ESKey.getParallelBulk(conf);
            if (parallelBulk < 1) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        String.format("parallelBulk must be at least 1, but is %d", parallelBulk));
            }
            if (parallelBulk > 1 && !ESKey.isMultiThread(conf)) {
                log.warn("the client is configured single threaded (multiThread=false), the bulk requests of "
                        + "parallelBulk={} are sent one after the other", parallelBulk);
            }
            if (parallelBulk > 32) {
                log.warn("parallelBulk={} keeps up to {} batches of {} records in memory",
                        parallelBulk, parallelBulk, batchSize);
            }
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
            // A full batch is handed to a worker and its result is collected once parallelBulk
            // batches are in flight: the round trip of one bulk then overlaps with the records
            // of the next, which is what a task otherwise waits for. With parallelBulk=1 this
            // is the loop that writes one batch and waits for it.
            //
            // The batches are applied in the order they are sent only while parallelBulk is 1:
            // two records carrying the same primary key in different batches may reach the
            // index in either order.
            ExecutorService workers = Executors.newFixedThreadPool(parallelBulk, runnable -> {
                Thread thread = new Thread(runnable, "es-bulk-" + index);
                thread.setDaemon(true);
                return thread;
            });
            Deque<Future<Long>> inFlight = new ArrayDeque<>();
            List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            long total = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    checkRecordShape(record);
                    writerBuffer.add(record);
                    if (writerBuffer.size() >= this.batchSize) {
                        inFlight.add(submit(workers, writerBuffer));
                        writerBuffer = new ArrayList<>(this.batchSize);
                        if (inFlight.size() >= parallelBulk) {
                            total += await(inFlight.poll());
                        }
                    }
                }

                if (!writerBuffer.isEmpty()) {
                    inFlight.add(submit(workers, writerBuffer));
                }
                while (!inFlight.isEmpty()) {
                    total += await(inFlight.poll());
                }
            }
            finally {
                workers.shutdownNow();
            }

            String msg = String.format("task end, write size :%d", total);
            getTaskPluginCollector().collectMessage("writeSize", String.valueOf(total));
            log.info(msg);
            esClient.closeJestClient();
        }

        /**
         * Hands one batch to a worker. The batch belongs to the worker from here on: the
         * caller starts a new list rather than clearing this one.
         */
        private Future<Long> submit(ExecutorService workers, List<Record> batch)
        {
            final List<Record> toWrite = batch;
            return workers.submit(() -> doBatchInsert(toWrite));
        }

        private static long await(Future<Long> pending)
        {
            try {
                return pending.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw AddaxException.asAddaxException(EXECUTE_FAIL, "interrupted while writing a batch", e);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw AddaxException.asAddaxException(EXECUTE_FAIL, cause);
            }
        }

        /**
         * Renders a date column as the ISO-8601 text elasticsearch parses without a format in
         * the mapping (its default is date_optional_time).
         *
         * <p>The millisecond part is always written: the values this used to produce with
         * joda-time carried it, and a downstream consumer may match on it.
         */
        /**
         * The columns of the writer are matched to the record by position, so a record of a
         * different width is a mistake in the job rather than something to write: without this
         * it surfaced as an IndexOutOfBoundsException in the middle of a batch, or as columns
         * that silently stayed empty.
         */
        private void checkRecordShape(Record record)
        {
            if (recordShapeChecked) {
                return;
            }
            recordShapeChecked = true;
            if (record.getColumnNumber() != columnList.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, String.format(
                        "the writer is configured with %d columns but the record has %d: the column list of the writer must match the columns of the reader",
                        columnList.size(), record.getColumnNumber()));
            }
        }

        private String getDateStr(ESColumn esColumn, Column column)
        {
            ZoneId zone = esColumn.getTimezone() != null
                    ? ZoneId.of(esColumn.getTimezone())
                    : ZoneId.systemDefault();
            if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
                TemporalAccessor parsed = DateTimeFormatter.ofPattern(esColumn.getFormat()).parse(column.asString());
                ZonedDateTime date;
                if (parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
                    // the format carried an offset or a zone of its own
                    date = ZonedDateTime.from(parsed);
                }
                else {
                    // a pattern may leave the time out ("yyyy-MM-dd"), and one that leaves the
                    // date out is read as the epoch day, the way a partial pattern used to be
                    // filled in
                    LocalDate day = parsed.isSupported(ChronoField.EPOCH_DAY)
                            ? LocalDate.from(parsed)
                            : LocalDate.ofEpochDay(0);
                    LocalTime time = parsed.isSupported(ChronoField.NANO_OF_DAY)
                            ? LocalTime.from(parsed)
                            : LocalTime.MIDNIGHT;
                    date = LocalDateTime.of(day, time).atZone(zone);
                }
                return date.format(ISO_DATE_TIME);
            }
            else if (column.getType() == Column.Type.DATE) {
                return Instant.ofEpochMilli(column.asLong()).atZone(zone).format(ISO_DATE_TIME);
            }
            else {
                return column.asString();
            }
        }

        private long doBatchInsert(final List<Record> writerBuffer)
        {
            Map<String, Object> data;
            final Bulk.Builder bulkAction = new Bulk.Builder().defaultIndex(this.index);
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
                                String idValue = record.getColumn(i).asString();
                                if (idValue == null) {
                                    if (!missingIdLogged) {
                                        missingIdLogged = true;
                                        log.warn("column[{}] is empty for a record, elasticsearch generates the id for it", columnName);
                                    }
                                }
                                else {
                                    id.append(idValue);
                                }
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
                                // the byte type is an integer of 8 bits, and a byte[] would
                                // reach elasticsearch base64 encoded
                                data.put(columnName, column.asLong());
                                break;
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

                if (id.length() == 0) {
                    // no id column, or no value in it: elasticsearch generates the id. A null
                    // primary key used to be written as the literal id "null" -- every such
                    // document landed on that one id and overwrote the previous one.
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
                        JestResult jestResult = esClient.bulkInsert(bulkAction);
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
                                log.warn("elasticsearch is overloaded ({}), the batch is sent again", status);
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
