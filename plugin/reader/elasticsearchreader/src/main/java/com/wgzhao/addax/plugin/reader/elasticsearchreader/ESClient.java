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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.ClearScroll;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.AliasMapping;
import io.searchbox.indices.aliases.GetAliases;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import io.searchbox.params.SearchType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author kesc
 * @since 2020-04-14 10:32
 */
public class ESClient
{
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private JestClient jestClient;

    public JestClient getClient()
    {
        return jestClient;
    }

    public void createClient(String endpoint,
            String user,
            String passwd,
            boolean multiThread,
            int readTimeout,
            boolean compression,
            boolean discovery)
    {

        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig.Builder(endpoint)
                .multiThreaded(multiThread)
                .connTimeout(30000)
                .readTimeout(readTimeout)
                .maxTotalConnection(200)
                .requestCompressionEnabled(compression)
                .discoveryEnabled(discovery)
                .discoveryFrequency(5L, TimeUnit.MINUTES);

        if (!user.isEmpty() && !passwd.isEmpty()) {
            httpClientConfig.defaultCredentials(user, passwd);
            httpClientConfig.setPreemptiveAuth(new HttpHost(endpoint));
        }

        factory.setHttpClientConfig(httpClientConfig.build());

        jestClient = factory.getObject();
    }

    public boolean indicesExists(String indexName)
            throws Exception
    {
        boolean isIndicesExists = false;
        JestResult rst = jestClient.execute(new IndicesExists.Builder(indexName).build());
        if (rst.isSucceeded()) {
            isIndicesExists = true;
        }
        else {
            switch (rst.getResponseCode()) {
                case 404:
                    isIndicesExists = false;
                    break;
                case 401:
                    // 无权访问
                default:
                    log.warn(rst.getErrorMessage());
                    break;
            }
        }
        return isIndicesExists;
    }

    public SearchResult search(String query,
            SearchType searchType,
            String index,
            String type,
            String scroll,
            Map<String, Object> headers,
            List<String> columns)
            throws IOException
    {
        Search.Builder searchBuilder = new Search.Builder(query)
                .setSearchType(searchType)
                .addIndex(index).addType(type).setHeader(headers);
        for (String column: columns) {
            searchBuilder.addSourceIncludePattern(column);
        }
        if (StringUtils.isNotBlank(scroll)) {
            searchBuilder.setParameter("scroll", scroll);
        }
        return jestClient.execute(searchBuilder.build());
    }

    public JestResult scroll(String scrollId, String scroll)
            throws Exception
    {
        SearchScroll.Builder builder = new SearchScroll.Builder(scrollId, scroll);
        return execute(builder.build());
    }

    public void clearScroll(String scrollId)
    {
        ClearScroll.Builder builder = new ClearScroll.Builder().addScrollId(scrollId);
        try {
            execute(builder.build());
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public JestResult execute(Action<JestResult> clientRequest)
            throws Exception
    {
        JestResult rst;
        rst = jestClient.execute(clientRequest);
        if (!rst.isSucceeded()) {
            log.warn(rst.getErrorMessage());
        }
        return rst;
    }

    public Integer getStatus(JestResult rst)
    {
        JsonObject jsonObject = rst.getJsonObject();
        if (jsonObject.has("status")) {
            return jsonObject.get("status").getAsInt();
        }
        return 600;
    }

    public boolean isBulkResult(JestResult rst)
    {
        JsonObject jsonObject = rst.getJsonObject();
        return jsonObject.has("items");
    }

    public boolean alias(String indexName, String aliasName, boolean needClean)
            throws IOException
    {
        GetAliases getAliases = new GetAliases.Builder().addIndex(aliasName).build();
        AliasMapping addAliasMapping = new AddAliasMapping.Builder(indexName, aliasName).build();
        JestResult rst = jestClient.execute(getAliases);
        log.info(rst.getJsonString());
        List<AliasMapping> list = new ArrayList<>();
        if (rst.isSucceeded()) {
            JsonParser jp = new JsonParser();
            JsonObject jo = (JsonObject) jp.parse(rst.getJsonString());
            for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
                String index = entry.getKey();
                if (indexName.equals(index)) {
                    continue;
                }
                AliasMapping m = new RemoveAliasMapping.Builder(index, aliasName).build();
                String s = new Gson().toJson(m.getData());
                log.info(s);
                if (needClean) {
                    list.add(m);
                }
            }
        }

        ModifyAliases modifyAliases = new ModifyAliases.Builder(addAliasMapping).addAlias(list).setParameter("master_timeout", "5m").build();
        rst = jestClient.execute(modifyAliases);
        if (!rst.isSucceeded()) {
            log.error(rst.getErrorMessage());
            return false;
        }
        return true;
    }

    /**
     * 关闭JestClient客户端
     */
    public void closeJestClient()
    {
        if (jestClient != null) {
            try {
                jestClient.close();
            }
            catch (IOException e) {
                log.warn("Failed to close es client:", e);
            }
        }
    }
}
