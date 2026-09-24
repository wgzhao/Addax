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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.AliasMapping;
import io.searchbox.indices.aliases.GetAliases;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by xiongfeng.bxf on 17/2/8.
 */
public class ESClient
{
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private JestClient jestClient;

    /** Returns the client. */
    public JestClient getClient()
    {
        return jestClient;
    }

    /** Createclient. */
    public void createClient(String endpoint,
            String user,
            String passwd,
            boolean multiThread,
            int readTimeout,
            boolean compression,
            boolean discovery)
    {

        JestClientFactory factory = new JestClientFactory();
        List<String> endpoints = Arrays.asList(endpoint.split(","));
        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig
                .Builder(endpoints)
                .multiThreaded(multiThread)
                .connTimeout(30000)
                .readTimeout(readTimeout)
                .maxTotalConnection(200)
                // apache httpclient caps a route at 2 connections of its own, which would keep
                // a task from having more than two bulk requests in flight however large
                // parallelBulk is
                .defaultMaxTotalConnectionPerRoute(200)
                .requestCompressionEnabled(compression)
                .discoveryEnabled(discovery)
                .discoveryFrequency(5L, TimeUnit.MINUTES);

        if (!("".equals(user) || "".equals(passwd))) {
            // HttpHost.create parses scheme, host and port out of the endpoint. The plain
            // HttpHost(String) constructor keeps the whole url as the host name, and the
            // credentials are then cached under a host the requests never match, so the
            // client falls back to answering a 401 challenge instead of authenticating up front.
            Set<HttpHost> hosts = endpoints.stream()
                    .map(String::trim)
                    .map(HttpHost::create)
                    .collect(Collectors.toSet());
            httpClientConfig.defaultCredentials(user, passwd).preemptiveAuthTargetHosts(hosts);
        }

        factory.setHttpClientConfig(httpClientConfig.build());

        jestClient = factory.getObject();
    }

    /**
     * Whether the index exists.
     *
     * <p>Only a 404 answers that question. Anything else (the credentials are refused, the
     * node is unreachable, a proxy answers instead) is raised: reporting "does not exist"
     * for it makes the writer try to create an index it cannot reach, and the error the
     * user gets names the wrong problem.
     */
    public boolean indicesExists(String indexName)
            throws Exception
    {
        JestResult rst = jestClient.execute(new IndicesExists.Builder(indexName).build());
        if (rst.isSucceeded()) {
            return true;
        }
        if (rst.getResponseCode() == 404) {
            return false;
        }
        throw new IOException(String.format("cannot read index[%s]: code:%s, msg:%s",
                indexName, rst.getResponseCode(), rst.getErrorMessage()));
    }

    /** Deleteindex. */
    public boolean deleteIndex(String indexName)
            throws Exception
    {
        log.info("delete index {}", indexName);
        if (indicesExists(indexName)) {
            JestResult rst = execute(new DeleteIndex.Builder(indexName).build());
            return rst.isSucceeded();
        }
        else {
            log.info("index cannot found, skip delete {}", indexName);
        }
        return true;
    }

    /**
     * Creates the index with its settings and mappings, and applies the mappings to an index
     * that is already there.
     *
     * <p>Elasticsearch 7.0 removed the mapping type: the index is created with its settings
     * and mappings in one request, and later mappings are put at {@code /{index}/_mapping}
     * (the type argument of the Jest actions stays empty, which is what makes them build
     * that path).
     */
    public boolean createIndex(String indexName, String mappings, String settings, boolean dynamic)
            throws Exception
    {
        JestResult rst;
        if (!indicesExists(indexName)) {
            log.info("create index {}", indexName);
            CreateIndex.Builder create = new CreateIndex.Builder(indexName)
                    .settings(settings)
                    .setParameter("master_timeout", "5m");
            if (!dynamic) {
                create.mappings(mappings);
            }
            rst = jestClient.execute(create.build());
            if (!rst.isSucceeded()) {
                if (isAlreadyExists(rst)) {
                    log.info("index [{}] already exists", indexName);
                    return true;
                }
                // a rejected settings or mappings body used to be reported as "already
                // exists", and the job then wrote into an index elasticsearch created on the
                // first document, with the mappings of the job silently dropped
                throw new IOException(String.format("cannot create index[%s]: %s", indexName, describe(rst)));
            }
            log.info("create [{}] index success", indexName);
            return true;
        }

        if (dynamic) {
            log.info("index [{}] exists, the mappings of the job are ignored (dynamic)", indexName);
            return true;
        }
        log.info("put mappings for {} {}", indexName, mappings);
        rst = jestClient.execute(new PutMapping.Builder(indexName, null, mappings)
                .setParameter("master_timeout", "5m").build());
        if (!rst.isSucceeded()) {
            if (getStatus(rst) == 400) {
                // a field of an existing index cannot be redefined: the mapping of the job
                // does not fully apply, and the writes that the difference breaks are the ones
                // elasticsearch rejects, one document at a time
                log.warn("the mappings of index [{}] do not apply: {}", indexName, describe(rst));
            }
            else {
                log.error(rst.getErrorMessage());
                return false;
            }
        }
        else {
            log.info("index [{}] put mappings success", indexName);
        }
        return true;
    }

    /** Execute. */
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

    /** Whether elasticsearch refused the request because the index is already there. */
    private static boolean isAlreadyExists(JestResult rst)
    {
        JsonObject error = errorOf(rst);
        return error != null && "resource_already_exists_exception".equals(error.get("type").getAsString());
    }

    /** The reason elasticsearch gives, response code included when there is no json body. */
    private static String describe(JestResult rst)
    {
        JsonObject error = errorOf(rst);
        if (error != null) {
            JsonElement reason = error.get("reason");
            return reason == null ? error.toString() : reason.getAsString();
        }
        String message = rst.getErrorMessage();
        return message == null ? "code:" + rst.getResponseCode() : message;
    }

    private static JsonObject errorOf(JestResult rst)
    {
        JsonObject jsonObject = rst.getJsonObject();
        if (jsonObject == null || !jsonObject.has("error")) {
            return null;
        }
        JsonElement error = jsonObject.get("error");
        return error.isJsonObject() ? error.getAsJsonObject() : null;
    }

    /** Returns the status. */
    public Integer getStatus(JestResult rst)
    {
        JsonObject jsonObject = rst.getJsonObject();
        if (jsonObject.has("status")) {
            return jsonObject.get("status").getAsInt();
        }
        return 600;
    }

    /** Checks whether the bulkresult condition holds. */
    public boolean isBulkResult(JestResult rst)
    {
        JsonObject jsonObject = rst.getJsonObject();
        return jsonObject.has("items");
    }

    /** Alias. */
    public void alias(String indexName, String aliasName, boolean needClean)
            throws IOException
    {
        GetAliases getAliases = new GetAliases.Builder().addIndex(aliasName).build();
        AliasMapping addAliasMapping = new AddAliasMapping.Builder(indexName, aliasName).build();
        JestResult rst = jestClient.execute(getAliases);
        log.info(rst.getJsonString());
        List<AliasMapping> list = new ArrayList<>();
        if (rst.isSucceeded()) {
            JsonObject jo = (JsonObject) JsonParser.parseString(rst.getJsonString());
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
            // the alias is what a downstream reader switches on: a job that reports success
            // while the alias still points at the previous index is worse than a failed job
            throw new IOException(String.format("cannot point alias[%s] at index[%s]: %s",
                    aliasName, indexName, describe(rst)));
        }
    }

    /**
     * Sends one bulk request.
     *
     * <p>The elasticsearch errors that are worth trying again (es_rejected_execution_exception,
     * cluster_block_exception) come back as a result with a failure inside, the retry is the
     * caller's.
     */
    public JestResult bulkInsert(Bulk.Builder bulk)
            throws Exception
    {
        JestResult rst;
        rst = jestClient.execute(bulk.build());
        if (!rst.isSucceeded()) {
            log.warn(rst.getErrorMessage());
        }
        return rst;
    }

    /** Closejestclient. */
    public void closeJestClient()
    {
        if (jestClient != null) {
            jestClient.shutdownClient();
        }
    }
}
