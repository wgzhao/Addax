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

package com.wgzhao.addax.core.transport.exchanger;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.CommonErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.transport.channel.Channel;
import com.wgzhao.addax.core.transport.record.TerminateRecord;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import com.wgzhao.addax.core.util.container.CoreConstant;

import java.util.List;

public class RecordExchanger
        extends TransformerExchanger
        implements RecordSender, RecordReceiver
{

    private static Class<? extends Record> RECORD_CLASS;
    private final Channel channel;
    private volatile boolean shutdown = false;

    @SuppressWarnings("unchecked")
    public RecordExchanger(int taskGroupId, int taskId, Channel channel, Communication communication,
            List<TransformerExecution> transformerExecs, TaskPluginCollector pluginCollector)
    {
        super(taskGroupId, taskId, communication, transformerExecs, pluginCollector);
        assert channel != null;
        this.channel = channel;
        Configuration configuration = channel.getConfiguration();
        try {
            RecordExchanger.RECORD_CLASS = (Class<? extends Record>) Class
                    .forName(configuration.getString(
                            CoreConstant.ADDAX_CORE_TRANSPORT_RECORD_CLASS,
                            "com.wgzhao.addax.core.transport.record.DefaultRecord"));
        }
        catch (ClassNotFoundException e) {
            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public Record getFromReader()
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        Record record = this.channel.pull();
        return (record instanceof TerminateRecord ? null : record);
    }

    @Override
    public Record createRecord()
    {
        try {
            return RECORD_CLASS.newInstance();
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record)
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        record = doTransformer(record);
        if (record == null) {
            return;
        }
        this.channel.push(record);
        //和channel的统计保持同步
        doStat();
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void terminate()
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        this.channel.pushTerminate(TerminateRecord.get());
        //和channel的统计保持同步
        doStat();
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
    }
}
