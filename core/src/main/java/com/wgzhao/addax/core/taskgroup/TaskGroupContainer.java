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

import com.wgzhao.addax.common.constant.PluginType;
import com.wgzhao.addax.common.exception.CommonErrorCode;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.statistics.PerfRecord;
import com.wgzhao.addax.common.statistics.PerfTrace;
import com.wgzhao.addax.common.statistics.VMInfo;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.AbstractContainer;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.communication.LocalTGCommunicationManager;
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
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import com.wgzhao.addax.core.util.TransformerUtil;
import com.wgzhao.addax.core.util.container.CoreConstant;
import com.wgzhao.addax.core.util.container.LoadUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TaskGroupContainer
        extends AbstractContainer
{
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private final long jobId;

    /**
     * 当前taskGroupId
     */
    private final int taskGroupId;

    /**
     * 使用的channel类
     */
    private final String channelClazz;

    /**
     * task收集器使用的类
     */
    private final String taskCollectorClass;

    private final TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration configuration)
    {
        super(configuration);

        initCommunicator(configuration);

        this.jobId = this.configuration.getLong(
                CoreConstant.ADDAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.ADDAX_CORE_CONTAINER_TASKGROUP_ID);

        this.channelClazz = this.configuration.getString(
                CoreConstant.ADDAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.ADDAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    private void initCommunicator(Configuration configuration)
    {
        super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));
    }

    public long getJobId()
    {
        return jobId;
    }

    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    @Override
    public void start()
    {
        try {

            // 状态check时间间隔，较短，可以把任务及时分发到对应channel中
            int sleepIntervalInMillSec = this.configuration.getInt(
                    CoreConstant.ADDAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);

            // 状态汇报时间间隔，稍长，避免大量汇报
            long reportIntervalInMillSec = this.configuration.getLong(
                    CoreConstant.ADDAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL,
                    10000);

            // 2分钟汇报一次性能统计

            // 获取channel数目
            int channelNumber = this.configuration.getInt(
                    CoreConstant.ADDAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            int taskMaxRetryTimes = this.configuration.getInt(
                    CoreConstant.ADDAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);

            long taskRetryIntervalInMsec = this.configuration.getLong(
                    CoreConstant.ADDAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);

            long taskMaxWaitInMsec = this.configuration.getLong(CoreConstant.ADDAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);

            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.ADDAX_JOB_CONTENT);

            if (LOG.isDebugEnabled()) {
                LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                        JSON.toJSONString(taskConfigs));
            }

            int taskCountInThisTaskGroup = taskConfigs.size();
            LOG.info("taskGroupId=[{}] start [{}] channels for [{}] tasks.",
                    this.taskGroupId, channelNumber, taskCountInThisTaskGroup);

            this.containerCommunicator.registerCommunication(taskConfigs);

            Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs); //taskId与task配置
            List<Configuration> taskQueue = buildRemainTasks(taskConfigs); //待运行task列表
            Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<>(); //taskId与上次失败实例
            List<TaskExecutor> runTasks = new ArrayList<>(channelNumber); //正在运行task
            Map<Integer, Long> taskStartTimeMap = new HashMap<>(); //任务开始时间

            long lastReportTimeStamp = 0;
            Communication lastTaskGroupContainerCommunication = new Communication();

            while (true) {
                //1.判断task状态
                boolean failedOrKilled = false;
                Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
                for (Map.Entry<Integer, Communication> entry : communicationMap.entrySet()) {
                    Integer taskId = entry.getKey();
                    Communication taskCommunication = entry.getValue();
                    if (!taskCommunication.isFinished()) {
                        continue;
                    }
                    TaskExecutor taskExecutor = removeTask(runTasks, taskId);

                    //上面从runTasks里移除了，因此对应在monitor里移除
                    taskMonitor.removeTask(taskId);

                    //失败，看task是否支持failover，重试次数未超过最大限制
                    if (taskCommunication.getState() == State.FAILED) {
                        taskFailedExecutorMap.put(taskId, taskExecutor);
                        assert taskExecutor != null;
                        if (taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes) {
                            taskExecutor.shutdown(); //关闭老的executor
                            containerCommunicator.resetCommunication(taskId); //将task的状态重置
                            Configuration taskConfig = taskConfigMap.get(taskId);
                            taskQueue.add(taskConfig); //重新加入任务列表
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
                            LOG.debug("taskGroup[{}] taskId[{}] is successful, used[{}]ms",
                                    this.taskGroupId, taskId, usedTime);
                            //usedTime*1000*1000 转换成PerfRecord记录的ns，这里主要是简单登记，进行最长任务的打印。因此增加特定静态方法
                            PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL, taskStartTime,
                                    usedTime * 1000L * 1000L);
                            taskStartTimeMap.remove(taskId);
                            taskConfigMap.remove(taskId);
                        }
                    }
                }

                // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
                if (failedOrKilled) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    throw AddaxException.asAddaxException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, lastTaskGroupContainerCommunication.getThrowable());
                }

                //3.有任务未执行，且正在运行的任务数小于最大通道限制
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
                        if (now - failedTime < taskRetryIntervalInMsec) {  //未到等待时间，继续留在队列
                            continue;
                        }
                        if (!lastExecutor.isShutdown()) { //上次失败的task仍未结束
                            if (now - failedTime > taskMaxWaitInMsec) {
                                markCommunicationFailed(taskId);
                                reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                                throw AddaxException.asAddaxException(CommonErrorCode.WAIT_TIME_EXCEED, "task failover等待超时");
                            }
                            else {
                                lastExecutor.shutdown(); //再次尝试关闭
                                continue;
                            }
                        }
                        else {
                            LOG.debug("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
                                    this.taskGroupId, taskId, lastExecutor.getAttemptCount());
                        }
                    }
                    Configuration taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
                    TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
                    taskStartTimeMap.put(taskId, System.currentTimeMillis());
                    taskExecutor.doStart();

                    iterator.remove();
                    runTasks.add(taskExecutor);

                    //上面，增加task到runTasks列表，因此在monitor里注册。
                    taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));

                    taskFailedExecutorMap.remove(taskId);
                    LOG.debug("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
                            this.taskGroupId, taskId, attemptCount);
                }

                //4.任务列表为空，executor已结束, 搜集状态为success--->成功
                if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
                    // 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    LOG.debug("taskGroup[{}] completed it's tasks.", this.taskGroupId);
                    break;
                }

                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    lastReportTimeStamp = now;

                    //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
                    for (TaskExecutor taskExecutor : runTasks) {
                        taskMonitor.report(taskExecutor.getTaskId(), this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
                    }
                }

                Thread.sleep(sleepIntervalInMillSec);
            }

            //6.最后还要汇报一次
            reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
        }
        catch (Throwable e) {
            Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);

            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }
        finally {
            if (!PerfTrace.getInstance().isJob()) {
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.debug(vmInfo.totalString());
                }

                LOG.debug(PerfTrace.getInstance().summarizeNoException());
                this.removeTaskGroup();//移除指定JobId中的统计Map
            }
        }
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
        return new LinkedList<>(configurations);
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

    public void removeTaskGroup()
    {
        try {

            // 移除根据JOBID做的一些标记 防止内存溢出
            LoadUtil.getConfigurationSet().remove(this.jobId);
            Iterator<Map.Entry<Integer, Communication>> it =
                    LocalTGCommunicationManager.getTaskGroupCommunicationMap().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Communication> entry = it.next();
                String strJobId = String.valueOf(this.jobId);
                String key = String.valueOf(entry.getKey());
                if (key.startsWith(strJobId)) {
                    it.remove();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TaskExecutor是一个完整task的执行器
     * 其中包括1：1的reader和writer
     */
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

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private final Communication taskCommunication;

        public TaskExecutor(Configuration taskConf, int attemptCount)
        {
            // 获取该taskExecutor的配置
            this.taskConfig = taskConf;
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = attemptCount;

            /*
             * 由taskId得到该taskExecutor的Communication
             * 要传给readerRunner和writerRunner，同时要传给channel作统计用
             */
            this.taskCommunication = containerCommunicator
                    .getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            /*
             * 获取transformer的参数
             */

            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            /*
             * 生成writerThread
             */
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
            this.writerThread = new Thread(writerRunner,
                    String.format("%d-%d-%d-writer",
                            jobId, taskGroupId, this.taskId));
            //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.WRITER, this.taskConfig.getString(
                            CoreConstant.JOB_WRITER_NAME), getJobId()));

            /*
             * 生成readerThread
             */
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);
            this.readerThread = new Thread(readerRunner,
                    String.format("%d-%d-%d-reader",
                            jobId, taskGroupId, this.taskId));
            /*
             * 通过设置thread的contextClassLoader，即可实现同步和主程序不同的加载器
             */
            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.READER, this.taskConfig.getString(
                            CoreConstant.JOB_READER_NAME), getJobId()));
        }

        public void doStart()
        {
            this.writerThread.start();

            // reader没有起来，writer不可能结束
            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw AddaxException.asAddaxException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

            this.readerThread.start();

            // 这里reader可能很快结束
            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw AddaxException.asAddaxException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }
        }

        private AbstractRunner generateRunner(PluginType pluginType)
        {
            return generateRunner(pluginType, null);
        }

        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs)
        {
            AbstractRunner newRunner;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                case READER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_READER_NAME), getJobId());
                    newRunner.setJobConf(this.taskConfig.getConfiguration(
                            CoreConstant.JOB_READER_PARAMETER));

                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.READER);

                    RecordSender recordSender;
                    if (transformerInfoExecs != null && !transformerInfoExecs.isEmpty()) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel, this.taskCommunication, pluginCollector, transformerInfoExecs);
                    }
                    else {
                        recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                    }

                    ((ReaderRunner) newRunner).setRecordSender(recordSender);

                    /*
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                case WRITER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME), getJobId());
                    newRunner.setJobConf(this.taskConfig
                            .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.WRITER);
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                            this.channel, pluginCollector));
                    /*
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                default:
                    throw AddaxException.asAddaxException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished()
        {
            // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
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
