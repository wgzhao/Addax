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

import com.wgzhao.addax.common.constant.PluginType;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Created by jingxing on 14-9-11.
 */
public abstract class AbstractTaskPluginCollector
        extends TaskPluginCollector
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskPluginCollector.class);

    private final Communication communication;

    private final Configuration configuration;

    private final PluginType pluginType;

    public AbstractTaskPluginCollector(Configuration conf, Communication communication, PluginType type)
    {
        this.configuration = conf;
        this.communication = communication;
        this.pluginType = type;
    }

    public Communication getCommunication()
    {
        return communication;
    }

    public Configuration getConfiguration()
    {
        return configuration;
    }

    public PluginType getPluginType()
    {
        return pluginType;
    }

    @Override
    public final void collectMessage(String key, String value)
    {
        this.communication.addMessage(key, value);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t, String errorMessage)
    {

        if (null == dirtyRecord) {
            LOG.warn("The dirty record is null.");
            return;
        }

        if (this.pluginType == PluginType.READER) {
            this.communication.increaseCounter(CommunicationTool.READ_FAILED_RECORDS, 1);
            this.communication.increaseCounter(CommunicationTool.READ_FAILED_BYTES, dirtyRecord.getByteSize());
        }
        else if (this.pluginType.equals(PluginType.WRITER)) {
            this.communication.increaseCounter(CommunicationTool.WRITE_FAILED_RECORDS, 1);
            this.communication.increaseCounter(CommunicationTool.WRITE_FAILED_BYTES, dirtyRecord.getByteSize());
        }
        else {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, "Unknown plugin type " + this.pluginType);
        }
    }
}
