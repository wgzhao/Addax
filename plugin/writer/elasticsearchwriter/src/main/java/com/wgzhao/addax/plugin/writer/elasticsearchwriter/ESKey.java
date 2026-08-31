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

import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** ESKey configuration keys. */
public final class ESKey
{

    /** Primary key column name. */
    public static final String PRIMARY_KEY_COLUMN_NAME = "pk";

    private ESKey() {}

    /** Returns the actiontype. */
    public static ActionType getActionType(Configuration conf)
    {
        String actionType = conf.getString("actionType", "index");
        if ("index".equals(actionType)) {
            return ActionType.INDEX;
        }
        else if ("create".equals(actionType)) {
            return ActionType.CREATE;
        }
        else if ("delete".equals(actionType)) {
            return ActionType.DELETE;
        }
        else if ("update".equals(actionType)) {
            return ActionType.UPDATE;
        }
        else {
            return ActionType.UNKNOWN;
        }
    }

    /** Returns the endpoint. */
    public static String getEndpoint(Configuration conf)
    {
        return conf.getNecessaryValue("endpoint", REQUIRED_VALUE);
    }

    /** Returns the accessid. */
    public static String getAccessID(Configuration conf)
    {
        return conf.getString("accessId", "");
    }

    /** Returns the accesskey. */
    public static String getAccessKey(Configuration conf)
    {
        return conf.getString("accessKey", "");
    }

    /** Returns the batchsize. */
    public static int getBatchSize(Configuration conf)
    {
        return conf.getInt("batchSize", 1000);
    }

    /** Returns the trysize. */
    public static int getTrySize(Configuration conf)
    {
        return conf.getInt("trySize", 30);
    }

    /** Returns the timeout. */
    public static int getTimeout(Configuration conf)
    {
        return conf.getInt("timeout", 600000);
    }

    /** Checks whether the cleanup condition holds. */
    public static boolean isCleanup(Configuration conf)
    {
        return conf.getBool("cleanup", false);
    }

    /** Checks whether the discovery condition holds. */
    public static boolean isDiscovery(Configuration conf)
    {
        return conf.getBool("discovery", false);
    }

    /** Checks whether the compression condition holds. */
    public static boolean isCompression(Configuration conf)
    {
        return conf.getBool("compression", true);
    }

    /** Checks whether the multithread condition holds. */
    public static boolean isMultiThread(Configuration conf)
    {
        return conf.getBool("multiThread", true);
    }

    /** Returns the indexname. */
    public static String getIndexName(Configuration conf)
    {
        return conf.getNecessaryValue("index", REQUIRED_VALUE);
    }

    /** Returns the typename. */
    public static String getTypeName(Configuration conf)
    {
        String indexType = conf.getString("indexType");
        if (StringUtils.isBlank(indexType)) {
            indexType = conf.getString("type", getIndexName(conf));
        }
        return indexType;
    }

    /** Checks whether the ignorewriteerror condition holds. */
    public static boolean isIgnoreWriteError(Configuration conf)
    {
        return conf.getBool("ignoreWriteError", false);
    }

    /** Checks whether the ignoreparseerror condition holds. */
    public static boolean isIgnoreParseError(Configuration conf)
    {
        return conf.getBool("ignoreParseError", true);
    }

    /** Checks whether the highspeedmode condition holds. */
    public static boolean isHighSpeedMode(Configuration conf)
    {
        return "high speed".equals(conf.getString("mode", ""));
    }

    /** Returns the alias. */
    public static String getAlias(Configuration conf)
    {
        return conf.getString("alias", "");
    }

    /** Checks whether the needcleanalias condition holds. */
    public static boolean isNeedCleanAlias(Configuration conf)
    {
        String mode = conf.getString("aliasMode", "append");
        return "exclusive".equals(mode);
    }

    /** Returns the settings. */
    public static Map<String, Object> getSettings(Configuration conf)
    {
        return conf.getMap("settings", new HashMap<>());
    }

    /** Returns the splitter. */
    public static String getSplitter(Configuration conf)
    {
        return conf.getString("splitter", "-,-");
    }

    /** Returns the dynamic. */
    public static boolean getDynamic(Configuration conf)
    {
        return conf.getBool("dynamic", false);
    }

    /** Action Type configuration keys. */
    public enum ActionType
    {
        UNKNOWN,
        INDEX,
        CREATE,
        DELETE,
        UPDATE
    }
}
