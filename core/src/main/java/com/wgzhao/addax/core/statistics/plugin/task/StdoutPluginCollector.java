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

package com.wgzhao.addax.core.statistics.plugin.task;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.common.constant.PluginType;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.plugin.task.util.DirtyRecord;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jingxing on 14-9-9.
 */
public class StdoutPluginCollector
        extends AbstractTaskPluginCollector
{
    private static final Logger LOG = LoggerFactory.getLogger(StdoutPluginCollector.class);

    private static final int DEFAULT_MAX_DIRTY_NUM = 128;
    private final AtomicInteger currentLogNum = new AtomicInteger(0);
    private AtomicInteger maxLogNum = new AtomicInteger(0);

    public StdoutPluginCollector(Configuration configuration, Communication communication, PluginType type)
    {
        super(configuration, communication, type);
        maxLogNum = new AtomicInteger(configuration.getInt(
                CoreConstant.CORE_STATISTICS_COLLECTOR_PLUGIN_MAX_DIRTY_NUMBER,
                DEFAULT_MAX_DIRTY_NUM));
    }

    private String formatDirty(final Record dirty, final Throwable t, final String msg)
    {
        Map<String, Object> msgGroup = new HashMap<>();

        msgGroup.put("type", super.getPluginType().toString());
        if (StringUtils.isNotBlank(msg)) {
            msgGroup.put("message", msg);
        }
        if (null != t && StringUtils.isNotBlank(t.getMessage())) {
            msgGroup.put("exception", t.getMessage());
        }
        if (null != dirty) {
            msgGroup.put("record", DirtyRecord.asDirtyRecord(dirty).getColumns());
        }

        return JSON.toJSONString(msgGroup);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t, String errorMessage)
    {
        int logNum = currentLogNum.getAndIncrement();
        if (logNum == 0 && t != null) {
            LOG.error("", t);
        }
        if (maxLogNum.intValue() < 0 || currentLogNum.intValue() < maxLogNum.intValue()) {
            LOG.error("The dirty data: {}", this.formatDirty(dirtyRecord, t, errorMessage));
        }

        super.collectDirtyRecord(dirtyRecord, t, errorMessage);
    }
}
