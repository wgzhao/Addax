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

package com.wgzhao.addax.core.transport.channel;

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.transport.record.TerminateRecord;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_CONTAINER_TASK_GROUP_ID;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_CHANNEL_CAPACITY;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_CHANNEL_FLOW_CONTROL_INTERVAL;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_BYTE;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_RECORD;

/**
 * The Channel is a queue between Reader and Writer.
 * it supports statistics and speed limit.
 */
public abstract class Channel
{

    private static final Logger LOG = LoggerFactory.getLogger(Channel.class);
    private static Boolean isFirstPrint = true;
    // last flow-control snapshot as primitives to avoid per-interval counter allocations
    private long lastTimestamp;
    private long lastReadBytes;
    private long lastReadFailedBytes;
    private long lastReadRecords;
    private long lastReadFailedRecords;
    protected int taskGroupId;
    protected int capacity;
    protected int byteCapacity;
    protected long byteSpeed; // bps: bytes/s
    protected long recordSpeed; // tps: records/s
    protected long flowControlInterval;
    protected volatile boolean isClosed = false;
    protected Configuration configuration;
    protected volatile AtomicLong waitReaderTime = new AtomicLong(0);
    protected volatile AtomicLong waitWriterTime = new AtomicLong(0);
    private Communication currentCommunication;
    private boolean waitCountersRegistered = false;

    public Channel(Configuration configuration)
    {
        int capacity = configuration.getInt(CORE_TRANSPORT_CHANNEL_CAPACITY, 2048);
        long byteSpeed = configuration.getLong(CORE_TRANSPORT_CHANNEL_SPEED_BYTE, 1024 * 1024L);
        long recordSpeed = configuration.getLong(CORE_TRANSPORT_CHANNEL_SPEED_RECORD, 10000L);

        if (capacity <= 0) {
            throw new IllegalArgumentException(String.format("The channel capacity [%d] must be greater than 0.", capacity));
        }

        if (isFirstPrint) {
            LOG.info("The Channel set byte_speed_limit to {}{}", byteSpeed, byteSpeed <= 0 ? ", No bps activated." : ".");
            LOG.info("The Channel set record_speed_limit to {}{}", recordSpeed, recordSpeed <= 0 ? ", No tps activated." : ".");
            isFirstPrint = false;
        }

        this.taskGroupId = configuration.getInt(CORE_CONTAINER_TASK_GROUP_ID);
        this.capacity = capacity;
        this.byteSpeed = byteSpeed;
        this.recordSpeed = recordSpeed;
        this.flowControlInterval = configuration.getLong(CORE_TRANSPORT_CHANNEL_FLOW_CONTROL_INTERVAL, 1000);
        this.byteCapacity = configuration.getInt(CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);
        this.configuration = configuration;
    }

    public void close()
    {
        this.isClosed = true;
    }

    public int getTaskGroupId()
    {
        return this.taskGroupId;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public long getByteSpeed()
    {
        return byteSpeed;
    }

    public Configuration getConfiguration()
    {
        return this.configuration;
    }

    public void setCommunication(final Communication communication)
    {
        this.currentCommunication = communication;
        this.lastTimestamp = System.currentTimeMillis();
        this.lastReadBytes = 0;
        this.lastReadFailedBytes = 0;
        this.lastReadRecords = 0;
        this.lastReadFailedRecords = 0;
        this.waitCountersRegistered = false;
    }

    public void push(Record r)
    {
        Validate.notNull(r, "The record cannot be empty.");
        this.doPush(r);
        this.statPush(1L, r.getByteSize());
    }

    public void pushTerminate(TerminateRecord r)
    {
        Validate.notNull(r, "The record cannot be empty.");
        this.doPush(r);
    }

    public void pushAll(Collection<Record> rs)
    {
        Validate.notNull(rs, "The Record must not be empty");
        Validate.noNullElements(rs);
        this.doPushAll(rs);
        this.statPush(rs.size(), this.getByteSize(rs));
    }

    public Record pull()
    {
        Record record = this.doPull();
        this.statPull(1L, record.getByteSize());
        return record;
    }

    public void pullAll(Collection<Record> rs)
    {
        Validate.notNull(rs, "The Record must not be empty");
        this.doPullAll(rs);
        this.statPull(rs.size(), this.getByteSize(rs));
    }

    protected abstract void doPush(Record r);

    protected abstract void doPushAll(Collection<Record> rs);

    protected abstract Record doPull();

    protected abstract void doPullAll(Collection<Record> rs);

    public abstract int size();

    public abstract boolean isEmpty();

    public abstract void clear();

    private long getByteSize(Collection<Record> rs)
    {
        long size = 0;
        for (Record each : rs) {
            size += each.getByteSize();
        }
        return size;
    }

    private void statPush(long recordSize, long byteSize)
    {
        currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_RECORDS, recordSize);
        currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_BYTES, byteSize);

        // register the live wait-time accumulators once per communication instead of
        // copying their values on every batch; waitReaderTime is written only by the
        // pull side and waitWriterTime only by the push side, so live reads are safe
        if (!waitCountersRegistered) {
            currentCommunication.putLongCounter(CommunicationTool.WAIT_READER_TIME, waitReaderTime);
            currentCommunication.putLongCounter(CommunicationTool.WAIT_WRITER_TIME, waitWriterTime);
            waitCountersRegistered = true;
        }

        boolean isChannelByteSpeedLimit = (this.byteSpeed > 0);
        boolean isChannelRecordSpeedLimit = (this.recordSpeed > 0);
        if (!isChannelByteSpeedLimit && !isChannelRecordSpeedLimit) {
            return;
        }

        long nowTimestamp = System.currentTimeMillis();
        long interval = nowTimestamp - this.lastTimestamp;
        if (interval - this.flowControlInterval >= 0) {
            long byteLimitSleepTime = 0;
            long recordLimitSleepTime = 0;
            if (isChannelByteSpeedLimit) {
                long currentByteSpeed = (CommunicationTool.getTotalReadBytes(currentCommunication) -
                        (this.lastReadBytes + this.lastReadFailedBytes)) * 1000 / interval;
                if (currentByteSpeed > this.byteSpeed) {
                    byteLimitSleepTime = currentByteSpeed * interval / this.byteSpeed - interval;
                }
            }

            if (isChannelRecordSpeedLimit) {
                long currentRecordSpeed = (CommunicationTool.getTotalReadRecords(currentCommunication) -
                        (this.lastReadRecords + this.lastReadFailedRecords)) * 1000 / interval;
                if (currentRecordSpeed > this.recordSpeed) {
                    recordLimitSleepTime = currentRecordSpeed * interval / this.recordSpeed - interval;
                }
            }

            long sleepTime = Math.max(byteLimitSleepTime, recordLimitSleepTime);
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // keep the snapshot in primitives instead of a per-interval Communication
            this.lastReadBytes = currentCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES);
            this.lastReadFailedBytes = currentCommunication.getLongCounter(CommunicationTool.READ_FAILED_BYTES);
            this.lastReadRecords = currentCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS);
            this.lastReadFailedRecords = currentCommunication.getLongCounter(CommunicationTool.READ_FAILED_RECORDS);
            this.lastTimestamp = nowTimestamp;
        }
    }

    private void statPull(long recordSize, long byteSize)
    {
        currentCommunication.increaseCounter(CommunicationTool.WRITE_RECEIVED_RECORDS, recordSize);
        currentCommunication.increaseCounter(CommunicationTool.WRITE_RECEIVED_BYTES, byteSize);
    }
}
