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

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.transport.channel.Channel;
import com.wgzhao.addax.core.transport.record.TerminateRecord;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.wgzhao.addax.core.spi.ErrorCode.OVER_LIMIT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_TRANSPORT_EXCHANGER_BUFFER_SIZE;


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
        this.bufferSize = configuration.getInt(CORE_TRANSPORT_EXCHANGER_BUFFER_SIZE, 32);

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
        lock.lock();
        try {
            this.queue.clear();
            // the dropped records will never be repaid by the consumer, so the byte budget
            // must be reset together with the queue it accounts for; otherwise a reused
            // channel inherits their debt and blocks every later push
            memoryBytes.set(0);
            notInsufficient.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    protected void doPush(Record r)
    {
        try {
            long startTime = System.nanoTime();
            this.queue.put(r);
            // blocked waiting for the writer to drain the queue
            waitWriterTime.addAndGet(System.nanoTime() - startTime);
            memoryBytes.addAndGet(r.getMemorySize());
        }
        catch (InterruptedException ex) {
            // propagate so the record is not counted as delivered; a silently
            // swallowed interrupt would leave the reader flag set and lose data
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        }
    }

    @Override
    protected void doPushAll(Collection<Record> rs)
    {
        try {
            long startTime = System.nanoTime();
            // A record larger than the whole byte budget can never be queued; failing here
            // turns a silent, permanent wait into a loud error (the exchanger rejects such
            // records earlier with the same rule, this is the primitive's own guard)
            for (Record r : rs) {
                int bytes = r.getMemorySize();
                if (bytes > this.byteCapacity) {
                    throw AddaxException.asAddaxException(OVER_LIMIT_ERROR, String.format(
                            "A single record requires %d bytes, which exceeds the transport channel byte capacity of %d. "
                                    + "Raise core.transport.channel.byteCapacity to accept records of this size.",
                            bytes, this.byteCapacity));
                }
            }

            lock.lockInterruptibly();
            // Enqueue record by record: requiring the whole batch to fit at once makes the
            // wait unsatisfiable whenever the batch exceeds the queue or the byte budget,
            // which hangs the producer forever. A draining consumer must always be able to
            // let the producer make progress.
            // One producer per channel and the consumer only removes records (clear()
            // included), so the queue can only get emptier behind this check
            for (Record r : rs) {
                int bytes = r.getMemorySize();
                while (memoryBytes.get() + bytes > this.byteCapacity || this.queue.remainingCapacity() == 0) {
                    notInsufficient.await(200L, TimeUnit.MILLISECONDS);
                }
                this.queue.add(r);
                memoryBytes.addAndGet(bytes);
                // wake a consumer parked on an empty queue right away: with a queue smaller
                // than the batch, the remaining records only arrive after it drains this one,
                // and waiting for the end of the batch would stall it on the 200 ms timeout
                notEmpty.signalAll();
            }
            // blocked waiting for the writer to drain the queue
            waitWriterTime.addAndGet(System.nanoTime() - startTime);
        }
        catch (InterruptedException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        finally {
            // lockInterruptibly may throw before the lock is acquired
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    protected Record doPull()
    {
        try {
            long startTime = System.nanoTime();
            Record r = this.queue.take();
            // blocked waiting for the reader to fill the queue
            waitReaderTime.addAndGet(System.nanoTime() - startTime);
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
            // blocked waiting for the reader to fill the queue
            waitReaderTime.addAndGet(System.nanoTime() - startTime);
            int bytes = getRecordBytes(rs);
            memoryBytes.addAndGet(-bytes);
            notInsufficient.signalAll();
        }
        catch (InterruptedException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
        finally {
            // lockInterruptibly may throw before the lock is acquired
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
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
