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

package com.wgzhao.addax.core.job.scheduler;

import com.wgzhao.addax.core.config.JobConfig;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.taskgroup.TaskGroupContainer;
import com.wgzhao.addax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.util.ErrorRecordChecker;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * The job scheduler: starts one thread per TaskGroupContainer and monitors the
 * aggregated job communication until all tasks finish or an error occurs.
 */
public class StandAloneScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger(StandAloneScheduler.class);

    private final AbstractContainerCommunicator containerCommunicator;

    private ExecutorService taskGroupContainerExecutorService;

    public StandAloneScheduler(AbstractContainerCommunicator containerCommunicator)
    {
        this.containerCommunicator = containerCommunicator;
    }

    /** Schedule. */
    public void schedule(List<JobConfig> configurations)
    {
        Validate.notNull(configurations, "The scheduler configuration cannot be empty");
        int jobReportIntervalInMillSec = configurations.get(0).getJobReportInterval();
        int jobSleepIntervalInMillSec = configurations.get(0).getJobSleepInterval();

        ErrorRecordChecker errorLimit = new ErrorRecordChecker(configurations.get(0).getErrorLimitRecord(),
                configurations.get(0).getErrorLimitPercentage());

        /*
         * Register communication channels for TaskGroupContainers
         */
        // the communicator registration boundary still speaks raw Configuration
        this.containerCommunicator.registerCommunication(configurations.stream()
                .map(JobConfig::getConfiguration)
                .toList());

        int totalTasks = calculateTaskCount(configurations);
        startAllTaskGroup(configurations);

        Communication lastJobContainerCommunication = new Communication();

        long lastReportTimeStamp = System.currentTimeMillis();
        while (true) {
            /*
             * step 1: collect job stat
             * step 2: getReport info, then report it
             * step 3: errorLimit do check
             * step 4: dealSucceedStat();
             * step 5: dealFailedStat();
             * step 6: refresh last job stat, and then sleep for next while
             */
            Communication nowJobContainerCommunication = this.containerCommunicator.collect();
            nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
            // toString walks every counter via reflection; build it only when debug is on
            if (LOG.isDebugEnabled()) {
                LOG.debug(nowJobContainerCommunication.toString());
            }

            long now = System.currentTimeMillis();
            if (now - lastReportTimeStamp > jobReportIntervalInMillSec) {
                Communication reportCommunication = CommunicationTool
                        .getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);

                this.containerCommunicator.report(reportCommunication);
                lastReportTimeStamp = now;
                // Update the last snapshot reference for next delta calculation
                lastJobContainerCommunication = nowJobContainerCommunication;
            }

            // Enforce error limits ASAP to fail-fast
            try {
                errorLimit.checkRecordLimit(nowJobContainerCommunication);
                errorLimit.checkPercentageLimit(nowJobContainerCommunication);
            }
            catch (Exception e) {
                // shut down the task-group executor before propagating the breach,
                // otherwise its non-daemon threads keep running in embedded use
                dealFailedStat(e);
            }

            State state = nowJobContainerCommunication.getState();
            if (state == State.SUCCEEDED) {
                LOG.info("The scheduler has completed all tasks.");
                break;
            }

            if (state == State.FAILED) {
                dealFailedStat(nowJobContainerCommunication.getThrowable());
            }

            try {
                TimeUnit.MILLISECONDS.sleep(jobSleepIntervalInMillSec);
            }
            catch (InterruptedException e) {
                // Restore interrupt status and exit as failed, shutting down task groups
                Thread.currentThread().interrupt();
                LOG.error("The scheduler thread was interrupted.", e);
                dealFailedStat(e);
            }
        }
    }

    private void startAllTaskGroup(List<JobConfig> configurations)
    {
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        for (JobConfig taskGroupConfiguration : configurations) {
            TaskGroupContainer taskGroupContainer = new TaskGroupContainer(taskGroupConfiguration);
            TaskGroupContainerRunner taskGroupContainerRunner = new TaskGroupContainerRunner(taskGroupContainer);
            this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
        }

        this.taskGroupContainerExecutorService.shutdown();
    }

    private void dealFailedStat(Throwable throwable)
    {
        this.taskGroupContainerExecutorService.shutdownNow();
        throw AddaxException.asAddaxException(
                RUNTIME_ERROR, throwable);
    }

    private int calculateTaskCount(List<JobConfig> configurations)
    {
        int totalTasks = 0;
        for (JobConfig taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getTaskConfigs().size();
        }
        return totalTasks;
    }
}
