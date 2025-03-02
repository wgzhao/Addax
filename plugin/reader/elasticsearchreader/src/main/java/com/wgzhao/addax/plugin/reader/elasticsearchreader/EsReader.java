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
import com.google.gson.JsonElement;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.statistics.PerfRecord;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.reader.elasticsearchreader.gson.MapTypeAdapter;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchResult;
import io.searchbox.params.SearchType;
import ognl.Ognl;
import ognl.OgnlContext;
import ognl.OgnlException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

/**
 * @author kesc mail:492167585@qq.com
 * @since 2020-04-14 10:32
 */

public class EsReader
        extends Reader
{

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
            String typeName = ESKey.getTypeName(conf);
            log.info("index:[{}], type:[{}]", indexName, typeName);
            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (!isIndicesExists) {
                    throw new IOException(String.format("index[%s] not exist", indexName));
                }
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
            List<Configuration> configurations = new ArrayList<>();
            List<Object> search = conf.getList(ESKey.SEARCH_KEY, Object.class);
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
            log.info("============elasticsearch reader job destroy=================");
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger log = LoggerFactory.getLogger(Task.class);
        private final OgnlContext ognlContext = new OgnlContext(null, null, new DefaultMemberAccess(true));
        ESClient esClient = null;
        Gson gson = null;
        private Configuration conf;
        private String index;
        private String type;
        private SearchType searchType;
        private Map<String, Object> headers;
        private String query;
        private String scroll;
        private List<String> column;
        private String filter;

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
            this.type = ESKey.getTypeName(conf);
            this.searchType = ESKey.getSearchType(conf);
            this.headers = ESKey.getHeaders(conf);
            this.query = ESKey.getQuery(conf);
            this.scroll = ESKey.getScroll(conf);
            this.filter = ESKey.getFilter(conf);
            this.column = ESKey.getColumn(conf);
            if (column == null || column.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "column is required");
            }
            if (column.size() == 1 && "*".equals(column.get(0))) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "The '*' is not supported");
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            //search
            PerfRecord queryPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();
            SearchResult searchResult;
            try {
                searchResult = esClient.search(query, searchType, index, type, scroll, headers, this.column);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
            if (!searchResult.isSucceeded()) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, searchResult.getResponseCode() + ":" + searchResult.getErrorMessage());
            }
            queryPerfRecord.end();
            //transport records
            PerfRecord allResultPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.RESULT_NEXT_ALL);
            allResultPerfRecord.start();
            this.transportRecords(recordSender, searchResult);
            allResultPerfRecord.end();
            //do scroll
            JsonElement scrollIdElement = searchResult.getJsonObject().get("_scroll_id");
            if (scrollIdElement == null) {
                return;
            }
            String scrollId = scrollIdElement.getAsString();
            log.debug("scroll id:{}", scrollId);
            try {
                boolean hasElement = true;
                while (hasElement) {
                    queryPerfRecord.start();
                    JestResult currScroll = esClient.scroll(scrollId, this.scroll);
                    queryPerfRecord.end();
                    if (!currScroll.isSucceeded()) {
                        throw AddaxException.asAddaxException(EXECUTE_FAIL,
                                String.format("scroll[id=%s] search error,code:%s,msg:%s", scrollId, currScroll.getResponseCode(), currScroll.getErrorMessage()));
                    }
                    allResultPerfRecord.start();
                    hasElement = this.transportRecords(recordSender, parseSearchResult(currScroll));
                    allResultPerfRecord.end();
                }
            }
            catch (AddaxException dxe) {
                throw dxe;
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, e);
            }
            finally {
                esClient.clearScroll(scrollId);
            }
        }

        private SearchResult parseSearchResult(JestResult jestResult)
        {
            if (jestResult == null) {
                return null;
            }
            SearchResult searchResult = new SearchResult(gson);
            searchResult.setSucceeded(jestResult.isSucceeded());
            searchResult.setResponseCode(jestResult.getResponseCode());
            searchResult.setPathToResult(jestResult.getPathToResult());
            searchResult.setJsonString(jestResult.getJsonString());
            searchResult.setJsonObject(jestResult.getJsonObject());
            searchResult.setErrorMessage(jestResult.getErrorMessage());
            return searchResult;
        }

        private Object getOgnlValue(Object expression, Map<String, Object> root, Object defaultValue)
        {
            try {
                if (!(expression instanceof String)) {
                    return defaultValue;
                }
                Object value = Ognl.getValue(expression.toString(), ognlContext, root);
                if (value == null) {
                    return defaultValue;
                }
                return value;
            }
            catch (OgnlException e) {
                return defaultValue;
            }
        }

        private boolean filter(String filter, String deleteFilterKey, Map<String, Object> record)
        {
            if (StringUtils.isNotBlank(deleteFilterKey)) {
                record.remove(deleteFilterKey);
            }
            if (StringUtils.isBlank(filter)) {
                return true;
            }
            return (Boolean) getOgnlValue(filter, record, Boolean.TRUE);
        }

        private boolean transportRecords(RecordSender recordSender, SearchResult result)
        {
            if (result == null) {
                return false;
            }
            List<String> sources = result.getSourceAsStringList();
            if (sources == null || sources.isEmpty()) {
                return false;
            }
            for (String source : sources) {
                this.transportOneRecord(recordSender, gson.fromJson(source, Map.class));
            }
            return true;
        }

        private void transportOneRecord(RecordSender recordSender, Map<String, Object> recordMap)
        {
            boolean allow = filter(this.filter, null, recordMap);
            if (allow && recordMap.entrySet().stream().anyMatch(x -> x.getValue() != null)) {
                Record record = recordSender.createRecord();
                boolean hasDirty = false;
                StringBuilder sb = new StringBuilder();
                for (String col: column) {
                    try {
                        Object o = recordMap.get(col);
                        record.addColumn(getColumn(o));
                    }
                    catch (Exception e) {
                        hasDirty = true;
                        sb.append(e);
                    }
                }
                if (hasDirty) {
                    getTaskPluginCollector().collectDirtyRecord(record, sb.toString());
                }
                recordSender.sendToWriter(record);
            }
        }

        private Column getColumn(Object value)
        {
            Column col;
            if (value == null) {
                col = new StringColumn();
            }
            else if (value instanceof String) {
                col = new StringColumn((String) value);
            }
            else if (value instanceof Integer) {
                col = new LongColumn(((Integer) value).longValue());
            }
            else if (value instanceof Long) {
                col = new LongColumn((Long) value);
            }
            else if (value instanceof Byte) {
                col = new LongColumn(((Byte) value).longValue());
            }
            else if (value instanceof Short) {
                col = new LongColumn(((Short) value).longValue());
            }
            else if (value instanceof Double) {
                col = new DoubleColumn(BigDecimal.valueOf((Double) value));
            }
            else if (value instanceof Float) {
                col = new DoubleColumn(BigDecimal.valueOf(((Float) value).doubleValue()));
            }
            else if (value instanceof Date) {
                col = new DateColumn((Date) value);
            }
            else if (value instanceof Boolean) {
                col = new BoolColumn((Boolean) value);
            }
            else if (value instanceof byte[]) {
                col = new BytesColumn((byte[]) value);
            }
            else if (value instanceof List) {
                col = new StringColumn(JSON.toJSONString(value));
            }
            else if (value instanceof Map) {
                col = new StringColumn(JSON.toJSONString(value));
            }
            else if (value instanceof Array) {
                col = new StringColumn(JSON.toJSONString(value));
            }
            else {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "type:" + value.getClass().getName());
            }
            return col;
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
