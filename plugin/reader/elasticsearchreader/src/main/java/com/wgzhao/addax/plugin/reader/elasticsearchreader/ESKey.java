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

import com.wgzhao.addax.core.util.Configuration;
import io.searchbox.params.SearchType;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

public final class ESKey
{
    public static final String SEARCH_KEY = "search";

    private ESKey() {}

    public static SearchType getSearchType(Configuration conf)
    {
        String searchType = conf.getString("searchType", SearchType.DFS_QUERY_THEN_FETCH.toString());
        return SearchType.valueOf(searchType.toUpperCase());
    }

    public static String getEndpoint(Configuration conf)
    {
        return conf.getNecessaryValue("endpoint", CONFIG_ERROR);
    }

    public static String getAccessID(Configuration conf)
    {
        return conf.getString("accessId", "");
    }

    public static String getAccessKey(Configuration conf)
    {
        return conf.getString("accessKey", "");
    }

    public static int getBatchSize(Configuration conf)
    {
        return conf.getInt("batchSize", 1000);
    }

    public static int getTrySize(Configuration conf)
    {
        return conf.getInt("trySize", 30);
    }

    public static int getTimeout(Configuration conf)
    {
        return conf.getInt("timeout", 60) * 1000;
    }

    public static boolean isCleanup(Configuration conf)
    {
        return conf.getBool("cleanup", false);
    }

    public static boolean isDiscovery(Configuration conf)
    {
        return conf.getBool("discovery", false);
    }

    public static boolean isCompression(Configuration conf)
    {
        return conf.getBool("compression", true);
    }

    public static boolean isMultiThread(Configuration conf)
    {
        return conf.getBool("multiThread", true);
    }

    public static String getIndexName(Configuration conf)
    {
        return conf.getNecessaryValue("index",  CONFIG_ERROR);
    }

    public static String getTypeName(Configuration conf)
    {
        String indexType = conf.getString("indexType");
        if (StringUtils.isBlank(indexType)) {
            indexType = conf.getString("type", getIndexName(conf));
        }
        return indexType;
    }

    public static Map<String, Object> getHeaders(Configuration conf)
    {
        return conf.getMap("headers", new HashMap<>());
    }

    public static String getQuery(Configuration conf)
    {
        return conf.getConfiguration(ESKey.SEARCH_KEY).toString();
    }

    public static String getScroll(Configuration conf)
    {
        return conf.getString("scroll");
    }

    public static List<String> getColumn(Configuration conf)
    {
        return conf.getList("column", String.class);
    }

    public static String getFilter(Configuration conf)
    {
        return conf.getString("filter", null);
    }
}
