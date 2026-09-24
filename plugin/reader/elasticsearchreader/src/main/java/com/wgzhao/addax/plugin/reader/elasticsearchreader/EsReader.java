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

package com.wgzhao.addax.plugin.reader.elasticsearchreader;

import com.alibaba.fastjson2.JSON;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.statistics.PerfRecord;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.reader.elasticsearchreader.gson.MapTypeAdapter;
import io.searchbox.client.JestResult;
import io.searchbox.params.SearchType;
import ognl.Ognl;
import ognl.OgnlContext;
import ognl.OgnlException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/**
 * @author kesc mail:492167585@qq.com
 * @since 2020-04-14 10:32
 */

public class EsReader
        extends Reader
{

    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void prepare()
        {
            ESClient esClient = new ESClient();
            esClient.createClient(ESKey.getEndpoint(conf),
                    ESKey.getAccessID(conf),
                    ESKey.getAccessKey(conf),
                    false,
                    300000,
                    false,
                    false);

            String indexName = ESKey.getIndexName(conf);
            log.info("index:[{}]", indexName);
            try {
                esClient.checkIndexExists(indexName);
            }
            catch (Exception ex) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, ex.toString());
            }
            esClient.closeJestClient();
        }

        @Override
        public void init()
        {
            this.conf = getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            // every query of the search array becomes a task. Check it here: a job without a
            // task fails later with "the number of tasks divided by the reader's job cannot be
            // less than or equal to zero", which says nothing about what is missing. The shape
            // matters too, a single query body is not an array of them.
            Object raw = conf.get(ESKey.SEARCH_KEY);
            if (raw == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "search is required: it holds the query body of every task");
            }
            if (!(raw instanceof List)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("search must be an array of query bodies, but is a %s", raw.getClass().getSimpleName()));
            }
            List<Object> search = conf.getList(ESKey.SEARCH_KEY, Object.class);
            if (search.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "search must not be empty");
            }
            List<Configuration> configurations = new ArrayList<>();
            for (Object query : search) {
                Configuration clone = conf.clone();
                clone.set(ESKey.SEARCH_KEY, query);
                configurations.add(clone);
            }
            return configurations;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private static final Type SOURCE_TYPE = new TypeToken<Map<String, Object>>() {}.getType();
        private final OgnlContext ognlContext = new OgnlContext(null, null, new DefaultMemberAccess(true));
        ESClient esClient = null;
        Gson gson = null;
        private Configuration conf;
        private String index;
        private SearchType searchType;
        private Map<String, Object> headers;
        private String query;
        private String scroll;
        private List<String> column;
        private String filter;
        /** The filter expression, parsed once: Ognl.getValue(String, ..) reparses on every call. */
        private Object filterExpression;
        private boolean filterErrorLogged;

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
        public void init()
        {
            this.conf = getPluginJobConf();
            this.esClient = new ESClient();
            this.gson = new GsonBuilder().registerTypeAdapterFactory(MapTypeAdapter.FACTORY).create();
            this.index = ESKey.getIndexName(conf);
            this.searchType = ESKey.getSearchType(conf);
            this.headers = ESKey.getHeaders(conf);
            this.scroll = ESKey.getScroll(conf);
            this.filter = ESKey.getFilter(conf);
            this.column = ESKey.getColumn(conf);
            this.query = withDefaultPageSize(ESKey.getQuery(conf));
            if (column == null || column.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "column is required");
            }
            if (column.size() == 1 && "*".equals(column.get(0))) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The '*' is not supported");
            }
            if (StringUtils.isNotBlank(this.filter)) {
                try {
                    this.filterExpression = Ognl.parseExpression(this.filter);
                }
                catch (OgnlException e) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("invalid filter expression: %s", this.filter), e);
                }
            }
        }

        /**
         * Fills in the page size of a scroll when the search body leaves it open.
         *
         * <p>Elasticsearch returns 10 documents per page by default, so a scroll without a size
         * costs one round trip per 10 documents. A size configured in the search body always
         * wins, and without scroll the body is left untouched: a single page request is a
         * legitimate way to read the first documents of an index.
         */
        private String withDefaultPageSize(String query)
        {
            if (query == null) {
                return null;
            }
            JsonObject body;
            try {
                JsonElement parsed = JsonParser.parseString(query);
                if (!parsed.isJsonObject()) {
                    return query;
                }
                body = parsed.getAsJsonObject();
            }
            catch (JsonParseException e) {
                // let elasticsearch report what is wrong with the body
                log.warn("search body is not valid JSON, passing it on unchanged: {}", e.getMessage());
                return query;
            }
            boolean hasSize = body.has("size") && !body.get("size").isJsonNull();
            if (StringUtils.isBlank(this.scroll)) {
                if (!hasSize) {
                    log.warn("no scroll is configured and the search body has no size: "
                            + "at most 10 documents will be read");
                }
                // a single page request is a legitimate way to read the first documents of an
                // index, so its body is left exactly as it was written
                return query;
            }
            if (hasSize) {
                log.info("search body sets the page size to {}", body.get("size").getAsString());
                return query;
            }
            int batchSize = ESKey.getBatchSize(this.conf);
            if (batchSize <= 0) {
                log.warn("batchSize={} is not a valid page size, leaving the search body unchanged", batchSize);
                return query;
            }
            body.addProperty("size", batchSize);
            log.info("search body has no size, using scroll page size batchSize={}", batchSize);
            return body.toString();
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            PerfRecord queryPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.SQL_QUERY);
            PerfRecord allResultPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.RESULT_NEXT_ALL);

            queryPerfRecord.start();
            JestResult page;
            try {
                page = esClient.search(query, searchType, index, scroll, headers, column);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
            queryPerfRecord.end();
            checkSucceeded(page, "search");

            String scrollId = scrollIdOf(page);
            if (scrollId == null) {
                long records = transportPage(recordSender, allResultPerfRecord, page);
                log.info("index[{}] read finished: {} records, no scroll was requested", index, records);
                return;
            }

            // A scroll is latency bound: every page costs a round trip plus the conversion of
            // the page. Fetching the next page on a background thread while the current one is
            // being converted overlaps the two, and only one request is ever in flight.
            ExecutorService fetcher = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "es-scroll-fetch");
                thread.setDaemon(true);
                return thread;
            });
            long total = 0;
            long pages = 0;
            try {
                Future<JestResult> inFlight = null;
                while (true) {
                    // elasticsearch may hand out a new scroll id with every page; use the latest
                    String nextScrollId = scrollIdOf(page);
                    if (nextScrollId != null) {
                        scrollId = nextScrollId;
                    }
                    // issue the next request before converting this page, so the wait for it
                    // overlaps with the conversion
                    if (hitCount(page) > 0) {
                        final String requestScrollId = scrollId;
                        inFlight = fetcher.submit(() -> esClient.scroll(requestScrollId, this.scroll));
                    }

                    total += transportPage(recordSender, allResultPerfRecord, page);

                    if (inFlight == null) {
                        // the page just converted was empty: the scroll is exhausted
                        break;
                    }
                    pages++;
                    queryPerfRecord.start();
                    page = await(inFlight);
                    queryPerfRecord.end();
                    inFlight = null;
                    checkSucceeded(page, "scroll");
                }
            }
            finally {
                fetcher.shutdownNow();
                esClient.clearScroll(scrollId);
            }
            log.info("index[{}] read finished: {} records in {} scroll pages", index, total, pages);
        }

        private long transportPage(RecordSender recordSender, PerfRecord perfRecord, JestResult page)
        {
            perfRecord.start();
            long records = transportRecords(recordSender, page);
            perfRecord.end();
            return records;
        }

        private static JestResult await(Future<JestResult> pending)
        {
            try {
                return pending.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw AddaxException.asAddaxException(EXECUTE_FAIL, "interrupted while fetching a scroll page", e);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw AddaxException.asAddaxException(EXECUTE_FAIL, cause);
            }
        }

        private static void checkSucceeded(JestResult result, String what)
        {
            if (!result.isSucceeded()) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        String.format("%s failed, code:%s, msg:%s", what, result.getResponseCode(), result.getErrorMessage()));
            }
        }

        private static String scrollIdOf(JestResult result)
        {
            JsonObject jsonObject = result.getJsonObject();
            if (jsonObject == null || !jsonObject.has("_scroll_id")) {
                return null;
            }
            JsonElement scrollId = jsonObject.get("_scroll_id");
            return scrollId.isJsonNull() ? null : scrollId.getAsString();
        }

        private static JsonArray hitsOf(JestResult result)
        {
            JsonObject jsonObject = result.getJsonObject();
            if (jsonObject == null || !jsonObject.has("hits")) {
                return null;
            }
            JsonElement hits = jsonObject.get("hits").getAsJsonObject().get("hits");
            return hits != null && hits.isJsonArray() ? hits.getAsJsonArray() : null;
        }

        private static int hitCount(JestResult result)
        {
            JsonArray hits = hitsOf(result);
            return hits == null ? 0 : hits.size();
        }

        /**
         * Converts every document of one response into a record.
         *
         * <p>The document itself is read straight from the parsed response: taking the {@code _source}
         * out as a string only to parse it again costs a serialization and a parse per document.
         *
         * @return the number of records handed to the writer
         */
        private long transportRecords(RecordSender recordSender, JestResult result)
        {
            JsonArray hits = hitsOf(result);
            if (hits == null) {
                return 0;
            }
            long sent = 0;
            for (JsonElement hit : hits) {
                JsonElement source = hit.getAsJsonObject().get("_source");
                if (source == null || source.isJsonNull()) {
                    // the query excluded _source, there is nothing to convert
                    continue;
                }
                Map<String, Object> recordMap = gson.fromJson(source, SOURCE_TYPE);
                if (recordMap != null && transportOneRecord(recordSender, recordMap)) {
                    sent++;
                }
            }
            return sent;
        }

        /**
         * Converts one document into a record and sends it, unless it is filtered out, empty or dirty.
         *
         * @return true if the record was sent to the writer
         */
        private boolean transportOneRecord(RecordSender recordSender, Map<String, Object> recordMap)
        {
            if (!matchesFilter(recordMap) || allValuesNull(recordMap)) {
                return false;
            }
            Record record = recordSender.createRecord();
            StringBuilder reasons = new StringBuilder();
            for (String col : column) {
                try {
                    record.addColumn(getColumn(recordMap.get(col)));
                }
                catch (Exception e) {
                    if (reasons.length() > 0) {
                        reasons.append("; ");
                    }
                    reasons.append("column[").append(col).append("]: ").append(e.getMessage());
                }
            }
            if (reasons.length() > 0) {
                // a partially built record is not sent to the writer, matching the other readers
                getTaskPluginCollector().collectDirtyRecord(record, reasons.toString());
                return false;
            }
            recordSender.sendToWriter(record);
            return true;
        }

        /**
         * Checks whether every value of the document is null.
         *
         * <p>The columns have already been narrowed down to the requested ones, so this is the
         * document that carries none of them: converting it would produce a row of empty columns.
         */
        private static boolean allValuesNull(Map<String, Object> recordMap)
        {
            for (Object value : recordMap.values()) {
                if (value != null) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Evaluates the filter expression against the document.
         *
         * <p>A filter that fails to evaluate keeps the record: a filter is a selection on top of
         * the query, and silently dropping the documents it cannot judge would hide the mistake.
         */
        private boolean matchesFilter(Map<String, Object> recordMap)
        {
            if (filterExpression == null) {
                return true;
            }
            Object value;
            try {
                value = Ognl.getValue(filterExpression, ognlContext, recordMap);
            }
            catch (OgnlException e) {
                if (!filterErrorLogged) {
                    filterErrorLogged = true;
                    log.warn("filter[{}] cannot be evaluated, the affected records are kept: {}", filter, e.getMessage());
                }
                return true;
            }
            if (value == null) {
                return true;
            }
            if (value instanceof Boolean b) {
                return b;
            }
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("filter[%s] must evaluate to a boolean, but returned a %s",
                            filter, value.getClass().getSimpleName()));
        }

        private static Column getColumn(Object value)
        {
            if (value == null) {
                return new StringColumn();
            }
            if (value instanceof String s) {
                return new StringColumn(s);
            }
            if (value instanceof Integer i) {
                return new LongColumn(i.longValue());
            }
            if (value instanceof Long l) {
                return new LongColumn(l);
            }
            if (value instanceof Byte b) {
                return new LongColumn(b.longValue());
            }
            if (value instanceof Short s) {
                return new LongColumn(s.longValue());
            }
            if (value instanceof Double d) {
                return new DoubleColumn(BigDecimal.valueOf(d));
            }
            if (value instanceof Float f) {
                return new DoubleColumn(BigDecimal.valueOf(f.doubleValue()));
            }
            if (value instanceof BigDecimal bigDecimal) {
                // an integer beyond the range of a long, kept as it was written in the document
                return new DoubleColumn(bigDecimal);
            }
            if (value instanceof Date d) {
                return new DateColumn(d);
            }
            if (value instanceof Boolean b) {
                return new BoolColumn(b);
            }
            if (value instanceof byte[] bytes) {
                return new BytesColumn(bytes);
            }
            if (value instanceof Number n) {
                return new DoubleColumn(new BigDecimal(n.toString()));
            }
            if (value instanceof Map || value instanceof List) {
                return new StringColumn(JSON.toJSONString(value));
            }
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "type:" + value.getClass().getName());
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            log.debug("============elasticsearch reader taskGroup[{}] taskId[{}] destroy=================", getTaskGroupId(), getTaskId());
            esClient.closeJestClient();
        }
    }
}
