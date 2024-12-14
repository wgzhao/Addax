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
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.spi.ErrorCode;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DorisWriterManager {

    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterManager.class);

    private final DorisStreamLoadObserver visitor;
    private final DorisKey options;
    private final List<byte[]> buffer = new ArrayList<> ();
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Throwable flushException;
    private final LinkedBlockingDeque< WriterTuple > flushQueue;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;

    public DorisWriterManager( DorisKey options) {
        this.options = options;
        this.visitor = new DorisStreamLoadObserver (options);
        flushQueue = new LinkedBlockingDeque<>(options.getFlushQueueLength());
        this.startScheduler();
        this.startAsyncFlushing();
    }

    public void startScheduler() {
        stopScheduler();
        this.scheduler = Executors.newScheduledThreadPool(1, new BasicThreadFactory.Builder().namingPattern("Doris-interval-flush").daemon(true).build());
        this.scheduledFuture = this.scheduler.schedule(() -> {
            synchronized (DorisWriterManager.this) {
                if (!closed) {
                    try {
                        String label = createBatchLabel();
                        LOG.info("Doris interval Sinking triggered: label[{}].", label);
                        if (batchCount == 0) {
                            startScheduler();
                        }
                        flush(label, false);
                    } catch (Throwable e) {
                        flushException = e;
                    }
                }
            }
        }, options.getFlushInterval(), TimeUnit.MILLISECONDS);
    }

    public void stopScheduler() {
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
    }

    public final synchronized void writeRecord(String record) {
        checkFlushException();
        try {
            byte[] bts = record.getBytes(StandardCharsets.UTF_8);
            buffer.add(bts);
            batchCount++;
            batchSize += bts.length;
            if (batchCount >= options.getBatchSize()) {
                String label = createBatchLabel();
                LOG.debug("Doris buffer Sinking triggered: rows[{}] label[{}].", batchCount, label);
                flush(label, false);
            }
        } catch (Exception e) {
            throw AddaxException.asAddaxException(ErrorCode.EXECUTE_FAIL, e);
        }
    }

    public synchronized void flush(String label, boolean waitUtilDone) throws Exception {
        checkFlushException();
        if (batchCount == 0) {
            if (waitUtilDone) {
                waitAsyncFlushingDone();
            }
            return;
        }
        flushQueue.put(new WriterTuple (label, batchSize,  new ArrayList<>(buffer)));
        if (waitUtilDone) {
            // wait the last flush
            waitAsyncFlushingDone();
        }
        buffer.clear();
        batchCount = 0;
        batchSize = 0;
    }

    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                String label = createBatchLabel();
                if (batchCount > 0) LOG.debug("Doris Sink is about to close: label[{}].", label);
                flush(label, true);
            } catch (Exception e) {
                throw new RuntimeException("Writing records to Doris failed.", e);
            }
        }
        checkFlushException();
    }

    public String createBatchLabel() {
        return options.getLabelPrefix() + UUID.randomUUID();
    }

    private void startAsyncFlushing() {
        // start flush thread
        Thread flushThread = new Thread(new Runnable(){
            public void run() {
                while(true) {
                    try {
                        asyncFlush();
                    } catch (Throwable e) {
                        flushException = e;
                    }
                }
            }
        });
        flushThread.setDaemon(true);
        flushThread.start();
    }

    private void waitAsyncFlushingDone() throws InterruptedException {
        // wait previous flushing
        for (int i = 0; i <= options.getFlushQueueLength(); i++) {
            flushQueue.put(new WriterTuple ("", 0L, null));
        }
        checkFlushException();
    }

    private void asyncFlush() throws Exception {
        WriterTuple flushData = flushQueue.take();
        if (Strings.isNullOrEmpty(flushData.getLabel())) {
            return;
        }
        stopScheduler();
        LOG.debug("Async stream load: rows[{}] bytes[{}] label[{}].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel());
        for (int i = 0; i <= options.getMaxRetries(); i++) {
            try {
                // flush to Doris with stream load
                visitor.streamLoad(flushData);
                LOG.info("Async stream load finished: label[{}].", flushData.getLabel());
                startScheduler();
                break;
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
