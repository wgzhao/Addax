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

package com.wgzhao.addax.core.taskgroup;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.constant.PluginType;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.spi.ErrorCode;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.AbstractContainer;
import com.wgzhao.addax.core.config.JobConfig;
import com.wgzhao.addax.core.config.TaskConfig;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.wgzhao.addax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.wgzhao.addax.core.taskgroup.runner.AbstractRunner;
import com.wgzhao.addax.core.taskgroup.runner.ReaderRunner;
import com.wgzhao.addax.core.taskgroup.runner.WriterRunner;
import com.wgzhao.addax.core.transport.channel.Channel;
import com.wgzhao.addax.core.transport.exchanger.BufferedRecordExchanger;
import com.wgzhao.addax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.util.ClassUtil;
import com.wgzhao.addax.core.util.TransformerUtil;
import com.wgzhao.addax.core.util.container.LoadUtil;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class TaskGroupContainer
        extends AbstractContainer
{
    private static final Logger LOG = LoggerFactory.getLogger(TaskGroupContainer.class);

    // The current taskGroupId
    private final int taskGroupId;

    // The channel class used
    private final String channelClazz;

    /**
     * The task Collector class used
     */
    private final String taskCollectorClass;

    private final TaskMonitor taskMonitor = TaskMonitor.getInstance();

    // wakes the scheduling loop as soon as some task reaches a finished state, so the next
    // task is dispatched without waiting for the next tick. This is a hint only: the loop
    // keeps deriving task state from the communication map, which stays the source of truth
    private final Semaphore taskCompletionSignal = new Semaphore(0);

    // typed reference to the communicator; the inherited containerCommunicator field
    // in AbstractContainer keeps pointing to the same instance
    private final StandaloneTGContainerCommunicator taskGroupCommunicator;

    // typed view over this task group's configuration; the inherited raw configuration
    // field in AbstractContainer keeps pointing to the same instance
    private final JobConfig jobConfig;

    /**
     * Construct a TaskGroupContainer with the given typed configuration view.
     *
     * @param jobConfig task group configuration view
     */
    public TaskGroupContainer(JobConfig jobConfig)
    {
        super(jobConfig.getConfiguration());

        this.jobConfig = jobConfig;
        this.taskGroupCommunicator = new StandaloneTGContainerCommunicator(jobConfig.getConfiguration());
        super.setContainerCommunicator(this.taskGroupCommunicator);

        this.taskGroupId = jobConfig.getTaskGroupId();
        this.channelClazz = jobConfig.getChannelClass();
        this.taskCollectorClass = jobConfig.getTaskCollectorClass();
    }

    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    /**
     * Start scheduling tasks within this task group, handling failover and reporting periodically.
     */
    @Override
    public void start()
    {
        // the interval time for checking the status of the task
        // it is short, so that the task can be distributed to the corresponding channel in time
        int sleepIntervalInMillSec = this.jobConfig.getTaskGroupSleepInterval();

        // the interval time for reporting the status of the task
        // it is long to avoid a large number of reports
        long reportIntervalInMillSec = this.jobConfig.getTaskGroupReportInterval();

        // the channel number may be absent; callers apply their own default
        Integer configuredChannelNumber = this.jobConfig.getTaskGroupChannel();
        int channelNumber = configuredChannelNumber != null ? configuredChannelNumber : 1;

        int taskMaxRetryTimes = this.jobConfig.getFailOverMaxRetryTimes();

        long taskRetryIntervalInMs = this.jobConfig.getFailOverRetryIntervalMs();

        long taskMaxWaitInMs = this.jobConfig.getFailOverMaxWaitMs();

        List<TaskConfig> taskConfigs = this.jobConfig.getTaskConfigs();

        // log the raw task configurations so the debug output keeps its original shape
        LOG.debug("The task configuration [{}] for taskGroup[{}]",
                JSON.toJSONString(taskConfigs.stream().map(TaskConfig::getConfiguration).toList()),
                this.taskGroupId);

        int taskCountInThisTaskGroup = taskConfigs.size();
        LOG.info("The taskGroupId=[{}] started [{}] channels for [{}] tasks.", this.taskGroupId, channelNumber, taskCountInThisTaskGroup);

        // the statistics register boundary still consumes raw configurations; the
        // communicator API is unchanged in this round
        this.taskGroupCommunicator.registerCommunication(taskConfigs.stream().map(TaskConfig::getConfiguration).toList());
        // set up the taskId and task configuration map
        Map<Integer, TaskConfig> taskConfigMap = buildTaskConfigMap(taskConfigs);
        // the task queue for the task that has not been executed
        List<TaskConfig> taskQueue = buildRemainTasks(taskConfigs);
        // the map for the task that has failed
        Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<>();
        // the list for the task that is running
        List<TaskExecutor> runTasks = new ArrayList<>(channelNumber);
        // the map for the task start time
        Map<Integer, Long> taskStartTimeMap = new HashMap<>();

        long lastReportTimeStamp = 0;
        Communication lastTaskGroupContainerCommunication = new Communication();

        try {
        while (true) {
            boolean failedOrKilled = false;
            Map<Integer, Communication> communicationMap = taskGroupCommunicator.getCommunicationMap();
            for (Map.Entry<Integer, Communication> entry : communicationMap.entrySet()) {
                Integer taskId = entry.getKey();
                Communication taskCommunication = entry.getValue();
                if (!taskCommunication.isFinished()) {
                    continue;
                }
                TaskExecutor taskExecutor = removeTask(runTasks, taskId);

                // the task has been removed from runTasks, so remove it from the monitor
                taskMonitor.removeTask(taskId);

                // if the task is failed, and the task supports fail over, and the attempt count is less than the max retry times
                if (taskCommunication.getState() == State.FAILED) {
                    taskFailedExecutorMap.put(taskId, taskExecutor);
                    assert taskExecutor != null;
                    if (taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes) {
                        taskExecutor.shutdown();
                        // reset the task status
                        taskGroupCommunicator.resetCommunication(taskId);
                        TaskConfig taskConfig = taskConfigMap.get(taskId);
                        taskQueue.add(taskConfig);
                    }
                    else {
                        failedOrKilled = true;
                        break;
                    }
                }
                else if (taskCommunication.getState() == State.SUCCEEDED) {
                    Long taskStartTime = taskStartTimeMap.get(taskId);
                    if (taskStartTime != null) {
                        long usedTime = System.currentTimeMillis() - taskStartTime;
                        LOG.debug("TaskGroup[{}] TaskId[{}] succeeded, used [{}]ms",
                                this.taskGroupId, taskId, usedTime);
                        taskStartTimeMap.remove(taskId);
                        taskConfigMap.remove(taskId);
                    }
                }
            }

            // if the task group has failed, then report the error
            if (failedOrKilled) {
                lastTaskGroupContainerCommunication = reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                throw AddaxException.asAddaxException(RUNTIME_ERROR, lastTaskGroupContainerCommunication.getThrowable());
            }

            // the task that has not been executed, and the number of running tasks is less than the channel number
            Iterator<TaskConfig> iterator = taskQueue.iterator();
            while (iterator.hasNext() && runTasks.size() < channelNumber) {
                TaskConfig taskConfig = iterator.next();
                Integer taskId = taskConfig.getTaskId();
                int attemptCount = 1;
                TaskExecutor lastExecutor = taskFailedExecutorMap.get(taskId);
                if (lastExecutor != null) {
                    attemptCount = lastExecutor.getAttemptCount() + 1;
                    long now = System.currentTimeMillis();
                    long failedTime = lastExecutor.getTimeStamp();
                    if (now - failedTime < taskRetryIntervalInMs) {
                        continue;
                    }
                    if (!lastExecutor.isShutdown()) {
                        if (now - failedTime > taskMaxWaitInMs) {
                            markCommunicationFailed(taskId);
                            reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                            throw AddaxException.asAddaxException(ErrorCode.WAIT_TIME_EXCEED, "The task fail over wait timed out.");
                        }
                        // the retry must not start while the previous attempt is still writing:
                        // a plugin that ignores interrupts would else run concurrently with it
                        // and duplicate rows. The wait is on the attempt's real termination and
                        // is bounded by the remaining fail-over budget; when the budget is gone
                        // this continues without waiting and the check above fails the task
                        lastExecutor.shutdownAndWait(Math.min(sleepIntervalInMillSec, taskMaxWaitInMs - (now - failedTime)));
                        continue;
                    }
                    else {
                        LOG.debug("TaskGroup[{}] TaskId[{}] AttemptCount[{}] has already shutdown",
                                this.taskGroupId, taskId, lastExecutor.getAttemptCount());
                    }
                }
                TaskConfig taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.copy() : taskConfig;
                TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
                taskStartTimeMap.put(taskId, System.currentTimeMillis());
                taskExecutor.doStart();

                iterator.remove();
                runTasks.add(taskExecutor);

                taskMonitor.registerTask(taskId, this.taskGroupCommunicator.getCommunication(taskId));

                taskFailedExecutorMap.remove(taskId);
                LOG.debug("TaskGroup[{}] TaskId[{}] AttemptCount[{}] has started",
                        this.taskGroupId, taskId, attemptCount);
            }

            // the task queue is empty, the executor has ended, and the collection status is success
            if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && taskGroupCommunicator.collectState() == State.SUCCEEDED) {
                lastTaskGroupContainerCommunication = reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                LOG.debug("The taskGroup[{}] has completed it's tasks.", this.taskGroupId);
                break;
            }

            // if the current time has exceeded the interval of the report time, we need to report immediately
            long now = System.currentTimeMillis();
            if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                lastTaskGroupContainerCommunication = reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                lastReportTimeStamp = now;

                // for the running tasks, check every reportIntervalInMillSec
                for (TaskExecutor taskExecutor : runTasks) {
                    taskMonitor.report(taskExecutor.getTaskId(), this.taskGroupCommunicator.getCommunication(taskExecutor.getTaskId()));
                }
            }
            try {
                // the tick timeout keeps the periodic duties (reporting, retry pacing) on their
                // original cadence, a finished task releases a permit and shortens the wait
                taskCompletionSignal.tryAcquire(sleepIntervalInMillSec, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Communication nowTaskGroupContainerCommunication = this.taskGroupCommunicator.collect();

                if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                    nowTaskGroupContainerCommunication.setThrowable(e);
                }
                nowTaskGroupContainerCommunication.setState(State.FAILED);
                this.taskGroupCommunicator.report(nowTaskGroupContainerCommunication);
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
            }
        }

        // final report for all taskGroup
        reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
        }
        catch (Throwable e) {
            // an exception escaping the loop before the first report would leave the
            // task-group communication RUNNING forever and hang the whole job; mark it
            // FAILED so the scheduler sees it within one collect cycle
            shutdownAll(runTasks, taskFailedExecutorMap);
            Communication comm = this.taskGroupCommunicator.collect();
            if (comm.getThrowable() == null) {
                comm.setThrowable(e);
            }
            comm.setState(State.FAILED);
            this.taskGroupCommunicator.report(comm);
            throw e instanceof AddaxException
                    ? (AddaxException) e
                    : AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    /**
     * Interrupt every running and failed task executor in the group.
     */
    private void shutdownAll(List<TaskExecutor> runTasks, Map<Integer, TaskExecutor> taskFailedExecutorMap)
    {
        for (TaskExecutor taskExecutor : runTasks) {
            taskExecutor.shutdown();
        }
        for (TaskExecutor taskExecutor : taskFailedExecutorMap.values()) {
            taskExecutor.shutdown();
        }
    }

    /**
     * Build a map from taskId to task configuration.
     */
    private Map<Integer, TaskConfig> buildTaskConfigMap(List<TaskConfig> taskConfigs)
    {
        Map<Integer, TaskConfig> map = new HashMap<>();
        for (TaskConfig taskConfig : taskConfigs) {
            int taskId = taskConfig.getTaskId();
            map.put(taskId, taskConfig);
        }
        return map;
    }

    /**
     * Build a list of pending tasks (a shallow copy of input list).
     */
    private List<TaskConfig> buildRemainTasks(List<TaskConfig> taskConfigs)
    {
        return new ArrayList<>(taskConfigs);
    }

    /**
     * Remove and return a TaskExecutor with given taskId from the running list.
     */
    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId)
    {
        Iterator<TaskExecutor> iterator = taskList.iterator();
        while (iterator.hasNext()) {
            TaskExecutor taskExecutor = iterator.next();
            if (taskExecutor.getTaskId() == taskId) {
                iterator.remove();
                return taskExecutor;
            }
        }
        return null;
    }

    /**
     * Check whether all tasks in the list have finished.
     */
    private boolean isAllTaskDone(List<TaskExecutor> taskList)
    {
        for (TaskExecutor taskExecutor : taskList) {
            if (!taskExecutor.isTaskFinished()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Report task group communication metrics and return the reported snapshot.
     */
    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, int taskCount)
    {
        Communication nowTaskGroupContainerCommunication = this.taskGroupCommunicator.collect();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication, taskCount);
        this.taskGroupCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId)
    {
        Communication communication = taskGroupCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }

    /**
     * Execute one attempt of a task: a paired reader/writer worker over a shared channel.
     */
    class TaskExecutor
    {
        private final TaskConfig taskConfig;

        private final int taskId;

        private final int attemptCount;

        private final Channel channel;

        // the attempt owns its executor: exactly the two workers it needs, and its termination is
        // the only honest answer to "both sides have really stopped". A Future flips to done the
        // moment cancel() interrupts the worker, long before the body returns, so it cannot stand
        // in for termination; the executor can, and waiting on it is bounded (no join(0) hang)
        private final ExecutorService workerExecutor;

        private final ClassLoader writerClassLoader;

        private final ClassLoader readerClassLoader;

        private final ReaderRunner readerRunner;

        private final WriterRunner writerRunner;

        // the taskCommunication for the taskExecutor will be invoked by channel, readerRunner, writerRunner
        // and the taskPluginCollector of reader and writer
        private final Communication taskCommunication;

        public TaskExecutor(TaskConfig taskConf, int attemptCount)
        {
            this.taskConfig = taskConf;
            Validate.isTrue(this.taskConfig.hasReader() && this.taskConfig.hasWriter(),
                    "The plugin parameters for reader and(or) writer cannot be empty!");

            this.taskId = this.taskConfig.getTaskId();
            this.attemptCount = attemptCount;

            // get the communication for the taskExecutor via taskId
            // then pass it to readerRunner and writerRunner, and pass it to channel for statistics
            this.taskCommunication = taskGroupCommunicator.getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    "Communication has not been registered for taskId:" + taskId);
            this.channel = ClassUtil.instantiate(channelClazz, Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER, null);
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);

            // resolve the plugin class loaders here, on the scheduling thread, and hand them to the
            // workers: each side of the attempt runs under its own plugin's class loader, and the
            // loaders of a pooled thread must not leak from one attempt into the next
            this.writerClassLoader = LoadUtil.getJarLoader(PluginType.WRITER, this.taskConfig.getWriterName());
            this.readerClassLoader = LoadUtil.getJarLoader(PluginType.READER, this.taskConfig.getReaderName());

            this.workerExecutor = Executors.newFixedThreadPool(2);
        }

        public void doStart()
        {
            // the writer goes first so it is ready to receive before the reader fills up the channel
            this.workerExecutor.execute(newWorker(this.writerRunner, "writer", this.writerClassLoader));
            this.workerExecutor.execute(newWorker(this.readerRunner, "reader", this.readerClassLoader));
            // the attempt submits exactly these two workers; from here on the executor only
            // tracks their termination, which is what isTaskFinished() and shutdownAndWait() wait for
            this.workerExecutor.shutdown();
        }

        /**
         * Wrap a runner into a worker of this attempt. The thread name and the plugin class loader
         * have to be installed inside the task body, because the thread belongs to the pool and
         * outlives nothing of its own.
         */
        private Runnable newWorker(Runnable runner, String side, ClassLoader pluginClassLoader)
        {
            return () -> {
                Thread.currentThread().setName(String.format("%s-%d-%d", side, taskGroupId, taskId));
                Thread.currentThread().setContextClassLoader(pluginClassLoader);
                try {
                    runner.run();
                }
                finally {
                    // a task that reached a finished state wakes the scheduling loop right away;
                    // the loop rereads the communication map, so a missed signal only costs a tick
                    if (this.taskCommunication.isFinished()) {
                        taskCompletionSignal.release();
                    }
                }
            };
        }

        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs)
        {
            AbstractRunner newRunner;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                case READER -> {
                    newRunner = LoadUtil.loadPluginRunner(pluginType, this.taskConfig.getReaderName());
                    newRunner.setJobConf(this.taskConfig.getReaderParameter());

                    pluginCollector = ClassUtil.instantiate(taskCollectorClass, AbstractTaskPluginCollector.class, configuration, this.taskCommunication, PluginType.READER);

                    RecordSender recordSender;
                    if (transformerInfoExecs != null && !transformerInfoExecs.isEmpty()) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel, this.taskCommunication, pluginCollector, transformerInfoExecs);
                    }
                    else {
                        recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                    }

                    ((ReaderRunner) newRunner).setRecordSender(recordSender);

                    // set the taskPlugin's collector to handle dirty data and job/task communication
                    newRunner.setTaskPluginCollector(pluginCollector);
                }
                case WRITER -> {
                    newRunner = LoadUtil.loadPluginRunner(pluginType, this.taskConfig.getWriterName());
                    newRunner.setJobConf(this.taskConfig.getWriterParameter());

                    pluginCollector = ClassUtil.instantiate(taskCollectorClass, AbstractTaskPluginCollector.class, configuration, this.taskCommunication, PluginType.WRITER);
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(this.channel, pluginCollector));

                    // set the taskPlugin's collector to handle dirty data and job/task communication
                    newRunner.setTaskPluginCollector(pluginCollector);
                }
                default -> throw AddaxException.asAddaxException(CONFIG_ERROR, "Cant generateRunner for:" + pluginType);
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        private boolean isTaskFinished()
        {
            // isTerminated() means both workers of the attempt have returned; the communication
            // alone cannot tell that, it is marked finished before the worker's teardown runs
            if (!this.workerExecutor.isTerminated()) {
                return false;
            }

            return taskCommunication != null && taskCommunication.isFinished();
        }

        private int getTaskId()
        {
            return taskId;
        }

        private long getTimeStamp()
        {
            return taskCommunication.getTimestamp();
        }

        private int getAttemptCount()
        {
            return attemptCount;
        }

        private boolean supportFailOver()
        {
            return writerRunner.supportFailOver();
        }

        private void shutdown()
        {
            // shut the transfer ends down first: clearing the channel releases a worker parked on
            // a full or empty queue, so the interrupt below lands on something that can react
            writerRunner.shutdown();
            readerRunner.shutdown();
            this.workerExecutor.shutdownNow();
        }

        /**
         * Shut down the attempt and wait up to {@code timeoutMs} for both workers to die.
         */
        private void shutdownAndWait(long timeoutMs)
        {
            this.shutdown();
            try {
                // a retry must not start while the previous attempt is still writing, so the wait
                // is on real termination; a timeout of zero returns at once and lets the caller
                // re-check the remaining fail-over budget instead of blocking forever
                this.workerExecutor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean isShutdown()
        {
            return this.workerExecutor.isTerminated();
        }
    }
}
