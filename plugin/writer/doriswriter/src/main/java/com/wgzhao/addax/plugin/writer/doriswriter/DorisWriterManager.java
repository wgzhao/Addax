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


package com.wgzhao.addax.plugin.writer.doriswriter;

import com.google.common.base.Strings;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.spi.ErrorCode;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Doris Writer Manager. */
public class DorisWriterManager {

    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterManager.class);

    private final DorisStreamLoadObserver visitor;
    private final DorisKey options;
    private final List<byte[]> buffer = new ArrayList<> ();
    private final LinkedBlockingDeque< WriterTuple > flushQueue;
    private final ScheduledExecutorService scheduler;
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Throwable flushException;

    /** Doriswritermanager. */
    public DorisWriterManager( DorisKey options) {
        this.options = options;
        this.visitor = new DorisStreamLoadObserver (options);
        flushQueue = new LinkedBlockingDeque<>(options.getFlushQueueLength());
        BasicThreadFactory basicThreadFactory = BasicThreadFactory.builder().namingPattern("Doris-interval-flush").daemon(true).build();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(basicThreadFactory);
        this.startAsyncFlushing();
        this.startScheduler();
    }

    /**
     * Starts the interval flush, which is a single fixed-delay task for the whole life of
     * the manager.  The previous implementation cancelled and rebuilt the scheduler around
     * every stream load, which both spawned a thread pool per batch and could shut down the
     * pool an interval task was being submitted to.
     */
    private void startScheduler() {
        this.scheduler.scheduleWithFixedDelay(() -> {
            synchronized (DorisWriterManager.this) {
                if (closed || batchCount == 0) {
                    return;
                }
                try {
                    String label = createBatchLabel();
                    LOG.debug("Doris interval Sinking triggered: rows[{}] bytes[{}] label[{}].", batchCount, batchSize, label);
                    flush(label);
                } catch (Throwable e) {
                    flushException = e;
                }
            }
        }, options.getFlushInterval(), options.getFlushInterval(), TimeUnit.MILLISECONDS);
    }

    /** Writerecord. */
    public final synchronized void writeRecord(String record) {
        checkFlushException();
        if (closed) {
            // the flush worker has already stopped, buffering now would silently drop data
            throw AddaxException.asAddaxException(ErrorCode.EXECUTE_FAIL,
                    new IOException("DorisWriterManager is already closed."));
        }
        try {
            byte[] bts = record.getBytes(StandardCharsets.UTF_8);
            buffer.add(bts);
            batchCount++;
            batchSize += bts.length;
            if (batchCount >= options.getBatchSize()) {
                String label = createBatchLabel();
                LOG.debug("Doris buffer Sinking triggered: rows[{}] bytes[{}] label[{}].", batchCount, batchSize, label);
                flush(label);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw AddaxException.asAddaxException(ErrorCode.EXECUTE_FAIL,
                    new IOException("Interrupted while writing records to Doris.", e));
        } catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.EXECUTE_FAIL, e);
        }
    }

    /**
     * Enqueues the buffered records as one batch and starts a fresh one.  Blocks while the
     * flush queue is full, which is what keeps the reader from outrunning the stream loads.
     */
    private synchronized void flush(String label) throws InterruptedException {
        if (batchCount == 0) {
            return;
        }
        flushQueue.put(new WriterTuple (label, batchSize,  new ArrayList<>(buffer)));
        buffer.clear();
        batchCount = 0;
        batchSize = 0;
    }

    /** Close. */
    public synchronized void close() {
        if (closed) {
            checkFlushException();
            return;
        }
        closed = true;
        scheduler.shutdown();
        try {
            if (batchCount > 0) {
                String label = createBatchLabel();
                LOG.debug("Doris Sink is about to close: rows[{}] label[{}].", batchCount, label);
                flush(label);
            }
            waitAsyncFlushingDone();
        } catch (Exception e) {
            throw new RuntimeException("Writing records to Doris failed.", e);
        } finally {
            try {
                visitor.close();
            } catch (IOException e) {
                LOG.warn("Failed to close the Doris stream load http client.", e);
            }
        }
        checkFlushException();
    }

    /** Createbatchlabel. */
    public String createBatchLabel() {
        return options.getLabelPrefix() + UUID.randomUUID();
    }

    private void startAsyncFlushing() {
        // start flush thread
        Thread flushThread = new Thread(() -> {
            while (true) {
                WriterTuple flushData;
                try {
                    flushData = flushQueue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (Strings.isNullOrEmpty(flushData.getLabel())) {
                    // barrier enqueued by waitAsyncFlushingDone(): due to FIFO ordering,
                    // all real batches enqueued before it have been stream-loaded
                    if (flushData.getLatch() != null) {
                        flushData.getLatch().countDown();
                    }
                    if (closed) {
                        return;
                    }
                    continue;
                }
                try {
                    asyncFlush(flushData);
                } catch (Throwable e) {
                    flushException = e;
                }
            }
        });
        flushThread.setDaemon(true);
        flushThread.start();
    }

    private void waitAsyncFlushingDone() throws InterruptedException {
        // enqueue a barrier and block until it is consumed: due to FIFO ordering,
        // all real batches enqueued before the barrier have been stream-loaded
        CountDownLatch latch = new CountDownLatch(1);
        flushQueue.put(new WriterTuple("", 0L, null, latch));
        latch.await();
        checkFlushException();
    }

    private void asyncFlush(WriterTuple flushData) throws Exception {
        LOG.debug("Async stream load: rows[{}] bytes[{}] label[{}].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel());
        for (int i = 0; i <= options.getMaxRetries(); i++) {
            try {
                // flush to Doris with stream load
                visitor.streamLoad(flushData);
                LOG.debug("Async stream load finished: label[{}].", flushData.getLabel());
                return;
            } catch (Exception e) {
                LOG.warn("Failed to flush batch data to Doris, retry times = {}", i, e);
                if (i >= options.getMaxRetries()) {
                    throw new IOException(e);
                }
                if (e instanceof DorisWriterException && ((DorisWriterException)e).needReCreateLabel()) {
                    String newLabel = createBatchLabel();
                    LOG.warn("Batch label changed from [{}] to [{}]", flushData.getLabel(), newLabel);
                    flushData.setLabel(newLabel);
                }
                TimeUnit.SECONDS.sleep(Math.min(i + 1, 10));
            }
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", flushException);
        }
    }
}
