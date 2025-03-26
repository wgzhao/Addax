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
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.AbstractContainer;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.wgzhao.addax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.wgzhao.addax.core.statistics.plugin.task.StdoutPluginCollector;
import com.wgzhao.addax.core.taskgroup.runner.AbstractRunner;
import com.wgzhao.addax.core.taskgroup.runner.ReaderRunner;
import com.wgzhao.addax.core.taskgroup.runner.WriterRunner;
import com.wgzhao.addax.core.transport.channel.Channel;
import com.wgzhao.addax.core.transport.channel.memory.MemoryChannel;
import com.wgzhao.addax.core.transport.exchanger.BufferedRecordExchanger;
import com.wgzhao.addax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.util.ClassUtil;
import com.wgzhao.addax.core.util.TransformerUtil;
import com.wgzhao.addax.core.util.container.CoreConstant;
import com.wgzhao.addax.core.util.container.LoadUtil;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class TaskGroupContainer
        extends AbstractContainer
{
    private static final Logger LOG = LoggerFactory.getLogger(TaskGroupContainer.class);

    // The current taskGroupId
    private final int taskGroupId;

    // The channel class used 使用的channel类
    private final String channelClazz;

    /**
     * The task Collector class used
     */
    private final String taskCollectorClass;

    private final TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration configuration)
    {
        super(configuration);

        initCommunicator(configuration);

        this.taskGroupId = this.configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_ID);
        this.channelClazz = this.configuration.getString(CoreConstant.CORE_TRANSPORT_CHANNEL_CLASS, MemoryChannel.class.getName());
        this.taskCollectorClass = this.configuration.getString(CoreConstant.CORE_STATISTICS_COLLECTOR_PLUGIN_TASK_CLASS, StdoutPluginCollector.class.getName());
    }

    private void initCommunicator(Configuration configuration)
    {
        super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));
    }

    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    @Override
    public void start()
    {
        // the interval time for checking the status of the task
        // it is short, so that the task can be distributed to the corresponding channel in time
        int sleepIntervalInMillSec = this.configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_SLEEP_INTERVAL, 100);

        // the interval time for reporting the status of the task
        // it is long to avoid a large number of reports
        long reportIntervalInMillSec = this.configuration.getLong(CoreConstant.CORE_CONTAINER_TASK_GROUP_REPORT_INTERVAL, 10000);

        int channelNumber = this.configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_CHANNEL, 1);

        int taskMaxRetryTimes = this.configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_FAIL_OVER_MAX_RETRY_TIMES, 1);

        long taskRetryIntervalInMs = this.configuration.getLong(CoreConstant.CORE_CONTAINER_TASK_FAIL_OVER_RETRY_INTERVAL_IN_MSEC, 10000);

        long taskMaxWaitInMs = this.configuration.getLong(CoreConstant.CORE_CONTAINER_TASK_FAIL_OVER_MAX_WAIT_IN_MSEC, 60000);

        List<Configuration> taskConfigs = this.configuration.getListConfiguration(CoreConstant.JOB_CONTENT);

        LOG.debug("The task configuration [{} for taskGroup[{}]", this.taskGroupId, JSON.toJSONString(taskConfigs));

        int taskCountInThisTaskGroup = taskConfigs.size();
        LOG.info("The taskGroupId=[{}] started [{}] channels for [{}] tasks.", this.taskGroupId, channelNumber, taskCountInThisTaskGroup);

        this.containerCommunicator.registerCommunication(taskConfigs);
        // setup the taskId and task configuration map
        Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs);
        // the task queue for the task that has not been executed
        List<Configuration> taskQueue = buildRemainTasks(taskConfigs);
        // the map for the task that has failed
        Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<>();
        // the list for the task that is running
        List<TaskExecutor> runTasks = new ArrayList<>(channelNumber);
        // the map for the task start time
        Map<Integer, Long> taskStartTimeMap = new HashMap<>();

        long lastReportTimeStamp = 0;
        Communication lastTaskGroupContainerCommunication = new Communication();

        while (true) {
            boolean failedOrKilled = false;
            Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
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
                        containerCommunicator.resetCommunication(taskId);
                        Configuration taskConfig = taskConfigMap.get(taskId);
                        taskQueue.add(taskConfig);
                    }
                    else {
                        failedOrKilled = true;
                        break;
                    }
                }
                else if (taskCommunication.getState() == State.KILLED) {
                    failedOrKilled = true;
                    break;
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
            Iterator<Configuration> iterator = taskQueue.iterator();
            while (iterator.hasNext() && runTasks.size() < channelNumber) {
                Configuration taskConfig = iterator.next();
                Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);
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
                        else {
                            lastExecutor.shutdown();
                            continue;
                        }
                    }
                    else {
                        LOG.debug("TaskGroup[{}] TaskId[{}] AttemptCount[{}] has already shutdown",
                                this.taskGroupId, taskId, lastExecutor.getAttemptCount());
                    }
                }
                Configuration taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
                TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
                taskStartTimeMap.put(taskId, System.currentTimeMillis());
                taskExecutor.doStart();

                iterator.remove();
                runTasks.add(taskExecutor);

                taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));

                taskFailedExecutorMap.remove(taskId);
                LOG.debug("TaskGroup[{}] TaskId[{}] AttemptCount[{}] has started",
                        this.taskGroupId, taskId, attemptCount);
            }

            // the task queue is empty, the executor has ended, and the collection status is success
            if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
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
                    taskMonitor.report(taskExecutor.getTaskId(), this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
                }
            }
            try {
                TimeUnit.MILLISECONDS.sleep(sleepIntervalInMillSec);
            }
            catch (InterruptedException e) {
                Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

                if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                    nowTaskGroupContainerCommunication.setThrowable(e);
                }
                nowTaskGroupContainerCommunication.setState(State.FAILED);
                this.containerCommunicator.report(nowTaskGroupContainerCommunication);
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
            }
        }

        // final report for all taskGroup
        reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
    }

    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations)
    {
        Map<Integer, Configuration> map = new HashMap<>();
        for (Configuration taskConfig : configurations) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            map.put(taskId, taskConfig);
        }
        return map;
    }

    private List<Configuration> buildRemainTasks(List<Configuration> configurations)
    {
        return new ArrayList<>(configurations);
    }

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

    private boolean isAllTaskDone(List<TaskExecutor> taskList)
    {
        for (TaskExecutor taskExecutor : taskList) {
            if (!taskExecutor.isTaskFinished()) {
                return false;
            }
        }
        return true;
    }

    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, int taskCount)
    {
        Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication, taskCount);
        this.containerCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId)
    {
        Communication communication = containerCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }

    class TaskExecutor
    {
        private final Configuration taskConfig;

        private final int taskId;

        private final int attemptCount;

        private final Channel channel;

        private final Thread readerThread;

        private final Thread writerThread;

        private final ReaderRunner readerRunner;

        private final WriterRunner writerRunner;

        // the taskCommunication for the taskExecutor will be invoked by channel, readerRunner, writerRunner
        // and the taskPluginCollector of reader and writer
        private final Communication taskCommunication;

        public TaskExecutor(Configuration taskConf, int attemptCount)
        {
            this.taskConfig = taskConf;
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "The plugin parameters for reader and(or) writer cannot be empty!");

            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = attemptCount;

            // get the communication for the taskExecutor via taskId
            // then pass it to readerRunner and writerRunner, and pass it to channel for statistics
            this.taskCommunication = containerCommunicator.getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    "Communication has not been registered for taskId:" + taskId);
            this.channel = ClassUtil.instantiate(channelClazz, Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER, null);
            this.writerThread = new Thread(writerRunner, String.format("writer-%d-%d", taskGroupId, this.taskId));
            // through setting the contextClassLoader of the thread, we can achieve the synchronization and the main program
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME)));

            readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);
            this.readerThread = new Thread(readerRunner, String.format("reader-%d-%d", taskGroupId, this.taskId));

            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.taskConfig.getString(CoreConstant.JOB_READER_NAME)));
        }

        public void doStart()
        {
            this.writerThread.start();

            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, this.taskCommunication.getThrowable());
            }

            this.readerThread.start();

            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, this.taskCommunication.getThrowable());
            }
        }

        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs)
        {
            AbstractRunner newRunner;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                case READER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType, this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(CoreConstant.JOB_READER_PARAMETER));

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
                    break;
                case WRITER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType, this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                    pluginCollector = ClassUtil.instantiate(taskCollectorClass, AbstractTaskPluginCollector.class, configuration, this.taskCommunication, PluginType.WRITER);
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(this.channel, pluginCollector));

                    // set the taskPlugin's collector to handle dirty data and job/task communication
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                default:
                    throw AddaxException.asAddaxException(CONFIG_ERROR, "Cant generateRunner for:" + pluginType);
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        private boolean isTaskFinished()
        {
            if (readerThread.isAlive() || writerThread.isAlive()) {
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
            writerRunner.shutdown();
            readerRunner.shutdown();
            if (writerThread.isAlive()) {
                writerThread.interrupt();
            }
            if (readerThread.isAlive()) {
                readerThread.interrupt();
            }
        }

        private boolean isShutdown()
        {
            return !readerThread.isAlive() && !writerThread.isAlive();
        }
    }
}
