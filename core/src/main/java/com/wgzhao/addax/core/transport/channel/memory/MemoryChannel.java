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

package com.wgzhao.addax.core.transport.channel.memory;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.transport.channel.Channel;
import com.wgzhao.addax.core.transport.record.TerminateRecord;
import com.wgzhao.addax.core.util.container.CoreConstant;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

/**
 * 内存Channel的具体实现，底层其实是一个ArrayBlockingQueue
 */
public class MemoryChannel
        extends Channel
{

    private final int bufferSize;

    private final AtomicInteger memoryBytes = new AtomicInteger(0);

    private final ArrayBlockingQueue<Record> queue;

    private final ReentrantLock lock;

    private final Condition notInsufficient;
    private final Condition notEmpty;

    public MemoryChannel(Configuration configuration)
    {
        super(configuration);
        this.queue = new ArrayBlockingQueue<>(this.getCapacity());
        this.bufferSize = configuration.getInt(CoreConstant.CORE_TRANSPORT_EXCHANGER_BUFFER_SIZE, 32);

        lock = new ReentrantLock();
        notInsufficient = lock.newCondition();
        notEmpty = lock.newCondition();
    }

    @Override
    public void close()
    {
        super.close();
        try {
            this.queue.put(TerminateRecord.get());
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void clear()
    {
        this.queue.clear();
    }

    @Override
    protected void doPush(Record r)
    {
        try {
            long startTime = System.nanoTime();
            this.queue.put(r);
            waitWriterTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(r.getMemorySize());
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void doPushAll(Collection<Record> rs)
    {
        try {
            long startTime = System.nanoTime();
            lock.lockInterruptibly();
            int bytes = getRecordBytes(rs);
            while (memoryBytes.get() + bytes > this.byteCapacity || rs.size() > this.queue.remainingCapacity()) {
                notInsufficient.await(200L, TimeUnit.MILLISECONDS);
            }
            this.queue.addAll(rs);
            waitWriterTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(bytes);
            notEmpty.signalAll();
        }
        catch (InterruptedException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    protected Record doPull()
    {
        try {
            long startTime = System.nanoTime();
            Record r = this.queue.take();
            waitReaderTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(-r.getMemorySize());
            return r;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void doPullAll(Collection<Record> rs)
    {
        assert rs != null;
        rs.clear();
        try {
            long startTime = System.nanoTime();
            lock.lockInterruptibly();
            while (this.queue.drainTo(rs, bufferSize) <= 0) {
                notEmpty.await(200L, TimeUnit.MILLISECONDS);
            }
            waitReaderTime += System.nanoTime() - startTime;
            int bytes = getRecordBytes(rs);
            memoryBytes.addAndGet(-bytes);
            notInsufficient.signalAll();
        }
        catch (InterruptedException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        finally {
            lock.unlock();
        }
    }

    private int getRecordBytes(Collection<Record> rs)
    {
        int bytes = 0;
        for (Record r : rs) {
            bytes += r.getMemorySize();
        }
        return bytes;
    }

    @Override
    public int size()
    {
        return this.queue.size();
    }

    @Override
    public boolean isEmpty()
    {
        return this.queue.isEmpty();
    }
}
