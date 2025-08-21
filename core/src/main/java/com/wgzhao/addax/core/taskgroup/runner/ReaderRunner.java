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

package com.wgzhao.addax.core.taskgroup.runner;

import com.wgzhao.addax.core.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.statistics.PerfRecord;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReaderRunner is responsible for executing a single Reader.Task slice.
 */
public class ReaderRunner
        extends AbstractRunner
        implements Runnable
{

    private static final Logger LOG = LoggerFactory.getLogger(ReaderRunner.class);

    private RecordSender recordSender;

    /**
     * Construct a ReaderRunner with a specific task plugin.
     *
     * @param abstractTaskPlugin the Reader.Task plugin instance
     */
    public ReaderRunner(AbstractTaskPlugin abstractTaskPlugin)
    {
        super(abstractTaskPlugin);
    }

    /**
     * Set the RecordSender used to send records downstream.
     *
     * @param recordSender the sender implementation
     */
    public void setRecordSender(RecordSender recordSender)
    {
        this.recordSender = recordSender;
    }

    @Override
    public void run()
    {
        assert null != this.recordSender;

        Reader.Task taskReader = (Reader.Task) this.getPlugin();

        PerfRecord channelWaitWrite = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.WAIT_WRITE_TIME);
        try {
            channelWaitWrite.start();

            LOG.debug("task reader starts to do init ...");
            PerfRecord initPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_INIT);
            initPerfRecord.start();
            taskReader.init();
            initPerfRecord.end();

            LOG.debug("task reader starts to do prepare ...");
            PerfRecord preparePerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_PREPARE);
            preparePerfRecord.start();
            taskReader.prepare();
            preparePerfRecord.end();

            LOG.debug("task reader starts to read ...");
            PerfRecord dataPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_DATA);
            dataPerfRecord.start();
            taskReader.startRead(recordSender);
            recordSender.terminate();

            dataPerfRecord.addCount(CommunicationTool.getTotalReadRecords(super.getRunnerCommunication()));
            dataPerfRecord.addSize(CommunicationTool.getTotalReadBytes(super.getRunnerCommunication()));
            dataPerfRecord.end();

            LOG.debug("task reader starts to do post ...");
            PerfRecord postPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_POST);
            postPerfRecord.start();
            taskReader.post();
            postPerfRecord.end();
        }
        catch (Throwable e) {
            LOG.error("Reader runner Received Exceptions:", e);
            super.markFail(e);
        }
        finally {
            LOG.debug("task reader starts to do destroy ...");
            PerfRecord desPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_DESTROY);
            desPerfRecord.start();
            super.destroy();
            desPerfRecord.end();

            channelWaitWrite.end(super.getRunnerCommunication().getLongCounter(CommunicationTool.WAIT_WRITER_TIME));

            long transformerUsedTime = super.getRunnerCommunication().getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME);
            if (transformerUsedTime > 0) {
                PerfRecord transformerRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.TRANSFORMER_TIME);
                transformerRecord.start();
                transformerRecord.end(transformerUsedTime);
            }
        }
    }

    /**
     * Shutdown the RecordSender if present to release resources.
     */
    public void shutdown()
    {
        if (recordSender != null) {
            recordSender.shutdown();
        }
    }
}
