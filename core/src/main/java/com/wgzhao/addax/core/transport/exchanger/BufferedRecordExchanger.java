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

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.transport.channel.Channel;
import com.wgzhao.addax.core.transport.record.TerminateRecord;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.SHUT_DOWN_TASK;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_EXCHANGER_BUFFER_SIZE;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_RECORD_CLASS;

/** Buffered Record Exchanger. */
public class BufferedRecordExchanger
        implements RecordSender, RecordReceiver
{

    protected final int byteCapacity;
    private final Channel channel;
    private final List<Record> buffer;
    private final AtomicInteger memoryBytes = new AtomicInteger(0);
    private final TaskPluginCollector pluginCollector;
    private final Supplier<Record> recordFactory;

    private static final Logger logger = LoggerFactory.getLogger(BufferedRecordExchanger.class);
    private int bufferSize;
    private int bufferIndex = 0;
    private volatile boolean shutdown = false;

    /** Bufferedrecordexchanger. */
    public BufferedRecordExchanger(Channel channel, TaskPluginCollector pluginCollector)
    {
        assert null != channel;
        assert null != channel.getConfiguration();

        this.channel = channel;
        this.pluginCollector = pluginCollector;
        Configuration configuration = channel.getConfiguration();

        this.bufferSize = configuration.getInt(CORE_TRANSPORT_EXCHANGER_BUFFER_SIZE, 32);
        this.buffer = new ArrayList<>(bufferSize);

        // The default channel queue capacity is 8MB (was 64MB)
        this.byteCapacity = configuration.getInt(
                CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);

        this.recordFactory = createRecordFactory(configuration);
    }

    @Override
    public Record createRecord()
    {
        return recordFactory.get();
    }

    /**
     * Resolve the record class once and cache its no-arg constructor so the
     * per-record creation path does no reflection lookups.
     */
    private static Supplier<Record> createRecordFactory(Configuration configuration)
    {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Record> recordClass = (Class<? extends Record>) Class
                    .forName(configuration.getString(
                            CORE_TRANSPORT_RECORD_CLASS,
                            "com.wgzhao.addax.core.transport.record.DefaultRecord"));
            Constructor<? extends Record> constructor = recordClass.getConstructor();
            return () -> {
                try {
                    return constructor.newInstance();
                }
                catch (ReflectiveOperationException e) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR, e);
                }
            };
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record)
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(SHUT_DOWN_TASK, "");
        }

        Validate.notNull(record, "The record cannot be empty.");

        if (record.getMemorySize() > this.byteCapacity) {
            this.pluginCollector.collectDirtyRecord(record,
                    new Exception(String.format("A single record exceeds the size limit. The current limit is %d", this.byteCapacity)));
            return;
        }

        boolean isFull = (this.bufferIndex >= this.bufferSize
                || this.memoryBytes.get() + record.getMemorySize() > this.byteCapacity);
        if (isFull) {
            flush();
        }

        this.buffer.add(record);
        this.bufferIndex++;
        memoryBytes.addAndGet(record.getMemorySize());
    }

    @Override
    public void flush()
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(SHUT_DOWN_TASK, "");
        }
        this.channel.pushAll(this.buffer);
        this.buffer.clear();
        this.bufferIndex = 0;
        this.memoryBytes.set(0);
    }

    @Override
    public void terminate()
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(SHUT_DOWN_TASK, "");
        }
        flush();
        this.channel.pushTerminate(TerminateRecord.get());
    }

    @Override
    public Record getFromReader()
    {
        if (shutdown) {
            throw AddaxException.asAddaxException(SHUT_DOWN_TASK, "");
        }
        boolean isEmpty = (this.bufferIndex >= this.buffer.size());
        if (isEmpty) {
            receive();
        }

        Record record = this.buffer.get(this.bufferIndex++);
        if (record instanceof TerminateRecord) {
            record = null;
        }
        return record;
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
        try {
            buffer.clear();
            channel.clear();
        }
        catch (Throwable t) {
            logger.error(t.getMessage());
        }
    }

    private void receive()
    {
        this.channel.pullAll(this.buffer);
        this.bufferIndex = 0;
        // do not overwrite bufferSize: it is the configured flush threshold for
        // the sender side, not the size of the last drained batch
    }
}
