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

package com.wgzhao.addax.core.job;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.common.constant.PluginType;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.AbstractJobPlugin;
import com.wgzhao.addax.common.plugin.JobPluginCollector;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.statistics.PerfTrace;
import com.wgzhao.addax.common.statistics.VMInfo;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.util.StrUtil;
import com.wgzhao.addax.core.AbstractContainer;
import com.wgzhao.addax.core.Engine;
import com.wgzhao.addax.core.container.util.JobAssignUtil;
import com.wgzhao.addax.core.hook.JobReport;
import com.wgzhao.addax.core.job.scheduler.AbstractScheduler;
import com.wgzhao.addax.core.job.scheduler.processinner.StandAloneScheduler;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.wgzhao.addax.core.statistics.plugin.DefaultJobPluginCollector;
import com.wgzhao.addax.core.util.ErrorRecordChecker;
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import com.wgzhao.addax.core.util.container.ClassLoaderSwapper;
import com.wgzhao.addax.core.util.container.CoreConstant;
import com.wgzhao.addax.core.util.container.LoadUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*
 * Created by jingxing on 14-8-24.
 * <p>
 * job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报
 * 但它并不做实际的数据同步操作
 */
public class JobContainer
        extends AbstractContainer
{
    private static final Logger LOG = LoggerFactory.getLogger(JobContainer.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
    private final ErrorRecordChecker errorLimit;
    private long jobId;
    private String readerPluginName;
    private String writerPluginName;
    /**
     * reader和writer jobContainer的实例
     */
    private Reader.Job jobReader;
    private Writer.Job jobWriter;
    private Configuration userConf;
    private long startTimeStamp;
    private long endTimeStamp;
    private long startTransferTimeStamp;
    private long endTransferTimeStamp;
    private int needChannelNumber;
    private int totalStage = 1;

    public JobContainer(Configuration configuration)
    {
        super(configuration);

        errorLimit = new ErrorRecordChecker(configuration);
    }

    /*
     * jobContainer主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
     * post以及destroy和statistics
     */
    @Override
    public void start()
    {
        LOG.info("Addax jobContainer starts job.");

        boolean hasException = false;
        boolean isDryRun = false;
        try {
            this.startTimeStamp = System.currentTimeMillis();
            isDryRun = configuration.getBool(CoreConstant.JOB_SETTING_DRY_RUN, false);
            if (isDryRun) {
                LOG.info("jobContainer starts to do preCheck ...");
                this.init();
                this.preCheck();
            }
            else {
                userConf = configuration.clone();
                LOG.debug("jobContainer starts to do preHandle ...");
                this.preHandle();

                LOG.debug("jobContainer starts to do init ...");
                this.init();
                LOG.debug("jobContainer starts to do prepare ...");
                this.prepare();
                LOG.debug("jobContainer starts to do split ...");
                this.totalStage = this.split();
                LOG.debug("jobContainer starts to do schedule ...");
                this.schedule();
                LOG.debug("jobContainer starts to do post ...");
                this.post();

                LOG.debug("jobContainer starts to do postHandle ...");
                this.postHandle();

                LOG.debug("Addax jobId [{}] completed successfully.", this.jobId);
                // disable hook function
                this.invokeHooks();
            }
        }
        catch (OutOfMemoryError e) {
            this.destroy();
        }
        catch (RuntimeException e) {

            hasException = true;

            if (super.getContainerCommunicator() == null) {
                // 由于 containerCollector 是在 scheduler() 中初始化的，所以当在 scheduler() 之前出现异常时，需要在此处对 containerCollector 进行初始化

                AbstractContainerCommunicator tempContainerCollector;
                // standalone
                tempContainerCollector = new StandAloneJobContainerCommunicator(configuration);

                super.setContainerCommunicator(tempContainerCollector);
            }

            Communication communication = super.getContainerCommunicator().collect();
            // 汇报前的状态，不需要手动进行设置
            // communication.setState(State.FAILED)
            communication.setThrowable(e);
            communication.setTimestamp(this.endTimeStamp);

            Communication tempComm = new Communication();
            tempComm.setTimestamp(this.startTransferTimeStamp);

            Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
            super.getContainerCommunicator().report(reportCommunication);

            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }
        finally {
            if (!isDryRun) {

                this.destroy();
                this.endTimeStamp = System.currentTimeMillis();
                if (!hasException) {
                    //最后打印cpu的平均消耗，GC的统计
                    VMInfo vmInfo = VMInfo.getVmInfo();
                    if (vmInfo != null) {
                        vmInfo.getDelta(false);
                        LOG.debug(vmInfo.totalString());
                    }

                    LOG.info(PerfTrace.getInstance().summarizeNoException());
                    this.logStatistics();
                }
            }
        }
    }

    private void preCheck()
    {
        this.preCheckInit();
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }
        this.preCheckReader();
        this.preCheckWriter();
        LOG.info("PreCheck通过");
    }

    private void preCheckInit()
    {
        this.jobId = this.configuration.getLong(
                CoreConstant.CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.CORE_CONTAINER_JOB_ID,
                    this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        this.jobReader = this.preCheckReaderInit(jobPluginCollector);
        this.jobWriter = this.preCheckWriterInit(jobPluginCollector);
    }

    private Reader.Job preCheckReaderInit(JobPluginCollector jobPluginCollector)
    {
        this.readerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName, this.jobId));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName, this.jobId);

        this.configuration.set(CoreConstant.JOB_CONTENT_READER_PARAMETER + ".dryRun", true);

        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));
        // 设置reader的readerConfig
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));
        this.configuration.set(CoreConstant.JOB_CONTENT_WRITER_PARAMETER + ".jobid", this.jobId);
        jobReader.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    private Writer.Job preCheckWriterInit(JobPluginCollector jobPluginCollector)
    {
        this.writerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName, this.jobId));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName, this.jobId);

        this.configuration.set(CoreConstant.JOB_CONTENT_WRITER_PARAMETER + ".dryRun", true);

        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));
        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void preCheckReader()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName, this.jobId));
        LOG.info("Addax Reader.Job [{}] do preCheck work .", this.readerPluginName);
        this.jobReader.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName, this.jobId));
        LOG.info("Addax Writer.Job [{}] do preCheck work .", this.writerPluginName);
        this.jobWriter.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /*
     * reader和writer的初始化
     */
    private void init()
    {
        this.jobId = this.configuration.getLong(CoreConstant.CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.CORE_CONTAINER_JOB_ID,
                    this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        //必须先Reader ，后Writer
        this.jobReader = this.initJobReader(jobPluginCollector);
        this.jobWriter = this.initJobWriter(jobPluginCollector);
    }

    private void prepare()
    {
        this.prepareJobReader();
        this.prepareJobWriter();
    }

    private void preHandle()
    {
        String handlerPluginTypeStr = this.configuration.getString(
                CoreConstant.JOB_PRE_HANDLER_PLUGIN_TYPE);
        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(
                CoreConstant.JOB_PRE_HANDLER_PLUGIN_NAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName, this.jobId));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName, this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        handler.preHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        LOG.info("After PreHandler: \n{}\n", Engine.filterJobConfiguration(configuration));
    }

    private void postHandle()
    {
        String handlerPluginTypeStr = this.configuration.getString(CoreConstant.JOB_POST_HANDLER_PLUGIN_TYPE);

        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job postHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(CoreConstant.JOB_POST_HANDLER_PLUGIN_NAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName, this.jobId));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName, this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        handler.postHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /*
     * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
     * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起，
     * 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
     */
    private int split()
    {
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }

        List<Configuration> readerTaskConfigs = this.doReaderSplit(this.needChannelNumber);
        int taskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this.doWriterSplit(taskNumber);

        List<Configuration> transformerList = this.configuration.getListConfiguration(CoreConstant.JOB_CONTENT_TRANSFORMER);

        LOG.debug("transformer configuration:{} ", JSON.toJSONString(transformerList));
        /*
         * 输入是reader和writer的parameter list，输出是content下面元素的list
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(readerTaskConfigs, writerTaskConfigs, transformerList);

        LOG.debug("contentConfig configuration:{} ", JSON.toJSONString(contentConfig));

        this.configuration.set(CoreConstant.JOB_CONTENT, contentConfig);

        return contentConfig.size();
    }

    private void adjustChannelNumber()
    {
        int needChannelNumberByByte = Integer.MAX_VALUE;
        int needChannelNumberByRecord = Integer.MAX_VALUE;

        boolean isByteLimit = (this.configuration.getInt(CoreConstant.JOB_SETTING_SPEED_BYTE, 0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = this.configuration.getInt(CoreConstant.JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);

            // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
            Long channelLimitedByteSpeed = this.configuration.getLong(CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_BYTE, -1);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                throw AddaxException.asAddaxException(
                        FrameworkErrorCode.CONFIG_ERROR,
                        "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            needChannelNumberByByte = (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte = needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to {} bytes.", globalLimitedByteSpeed);
        }

        boolean isRecordLimit = (this.configuration.getInt(CoreConstant.JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = this.configuration.getInt(CoreConstant.JOB_SETTING_SPEED_RECORD, 100000);
            Long channelLimitedRecordSpeed = this.configuration.getLong(CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_RECORD, -1);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                throw AddaxException.asAddaxException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord = (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to {} records.", globalLimitedRecordSpeed);
        }

        // 取较小值
        this.needChannelNumber = Math.min(needChannelNumberByByte, needChannelNumberByRecord);

        // 如果从byte或record上设置了needChannelNumber则退出
        if (this.needChannelNumber < Integer.MAX_VALUE) {
            return;
        }

        this.needChannelNumber = this.configuration.getInt(CoreConstant.JOB_SETTING_SPEED_CHANNEL, 1);
        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }
        LOG.info("Job set Channel-Number to {} channel(s).", this.needChannelNumber);
    }

    /*
     * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroupContainer中,
     * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
     */
    private void schedule()
    {
        /*
         * 这里的全局speed和每个channel的速度设置为B/s
         */
        int channelsPerTaskGroup = this.configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_CHANNEL, 5);
        int taskNumber = this.configuration.getList(CoreConstant.JOB_CONTENT).size();

        this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
        PerfTrace.getInstance().setChannelNumber(needChannelNumber);

        /*
         * 通过获取配置信息得到每个taskGroup需要运行哪些tasks任务
         */

        List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.configuration, this.needChannelNumber, channelsPerTaskGroup);

        LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());

        AbstractScheduler scheduler;
        try {
            scheduler = initStandaloneScheduler(this.configuration);
            this.startTransferTimeStamp = System.currentTimeMillis();
            scheduler.schedule(taskGroupConfigs);
            this.endTransferTimeStamp = System.currentTimeMillis();
        }
        catch (Exception e) {
            LOG.error("运行scheduler出错.");
            this.endTransferTimeStamp = System.currentTimeMillis();
            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

        /*
         * 检查任务执行情况
         */
        this.checkLimit();
    }

    private AbstractScheduler initStandaloneScheduler(Configuration configuration)
    {
        AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
        super.setContainerCommunicator(containerCommunicator);

        return new StandAloneScheduler(containerCommunicator);
    }

    private void post()
    {
        this.postJobWriter();
        this.postJobReader();
    }

    private void destroy()
    {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }
        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }
    }

    private void logStatistics()
    {
        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
        long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;
        if (0L == transferCosts) {
            transferCosts = 1L;
        }

        if (super.getContainerCommunicator() == null) {
            return;
        }

        Communication communication = super.getContainerCommunicator().collect();
        communication.setTimestamp(this.endTimeStamp);

        Communication tempComm = new Communication();
        tempComm.setTimestamp(this.startTransferTimeStamp);

        Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);

        // 字节速率
        long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES) / transferCosts;
        long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS) / transferCosts;

        reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
        reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

        super.getContainerCommunicator().report(reportCommunication);
        LOG.debug(CommunicationTool.Stringify.getSnapshot(communication));

        LOG.info(String.format(
                "\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n",
                "任务启动时刻",
                dateFormat.format(startTimeStamp),

                "任务结束时刻",
                dateFormat.format(endTimeStamp),

                "任务总计耗时",
                totalCosts + "s",
                "任务平均流量",
                StrUtil.stringify(byteSpeedPerSecond)
                        + "/s",
                "记录写入速度",
                recordSpeedPerSecond
                        + "rec/s", "读出记录总数",
                CommunicationTool.getTotalReadRecords(communication),
                "读写失败总数",
                CommunicationTool.getTotalErrorRecords(communication)
        ));

        if (communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
            LOG.info(String.format(
                    "\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n",
                    "Transformer成功记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS),

                    "Transformer失败记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS),

                    "Transformer过滤记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)
            ));
        }
    }

    /*
     * reader job的初始化，返回Reader.Job
     */
    private Reader.Job initJobReader(
            JobPluginCollector jobPluginCollector)
    {
        this.readerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName, this.jobId));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName, this.jobId);
        this.configuration.set(CoreConstant.JOB_CONTENT_READER_PARAMETER_JOB_ID, this.jobId);
        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));

        // 设置reader的readerConfig
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));

        jobReader.setJobPluginCollector(jobPluginCollector);
        jobReader.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    /*
     * writer job的初始化，返回Writer.Job
     */
    private Writer.Job initJobWriter(JobPluginCollector jobPluginCollector)
    {
        this.writerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName, this.jobId));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName, this.jobId);
        this.configuration.set(CoreConstant.JOB_CONTENT_WRITER_PARAMETER_JOB_ID, this.jobId);
        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));

        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void prepareJobReader()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName, this.jobId));
        LOG.info("Addax Reader.Job [{}] do prepare work .", this.readerPluginName);
        this.jobReader.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName, this.jobId));
        LOG.info("Addax Writer.Job [{}] do prepare work .", this.writerPluginName);
        this.jobWriter.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private List<Configuration> doReaderSplit(int adviceNumber)
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName, this.jobId));
        List<Configuration> readerSlicesConfigs = this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.isEmpty()) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "reader切分的task数目不能小于等于0");
        }
        LOG.info("Addax Reader.Job [{}] splits to [{}] tasks.", this.readerPluginName, readerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber)
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName, this.jobId));

        List<Configuration> writerSlicesConfigs = this.jobWriter.split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.isEmpty()) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "writer切分的task不能小于等于0");
        }
        LOG.info("Addax Writer.Job [{}] splits to [{}] tasks.", this.writerPluginName, writerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    /*
     * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs,
            List<Configuration> transformerConfigs)
    {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw AddaxException.asAddaxException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            readerTasksConfigs.size(), writerTasksConfigs.size())
            );
        }

        List<Configuration> contentConfigs = new ArrayList<>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            taskConfig.set(CoreConstant.JOB_READER_NAME, this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER, readerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_WRITER_NAME, this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER, writerTasksConfigs.get(i));

            if (transformerConfigs != null && !transformerConfigs.isEmpty()) {
                taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            taskConfig.set(CoreConstant.TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        return contentConfigs;
    }

    private void postJobReader()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName, this.jobId));
        LOG.info("Addax Reader.Job [{}] do post work.", this.readerPluginName);
        this.jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName, this.jobId));
        LOG.info("Addax Writer.Job [{}] do post work.", this.writerPluginName);
        this.jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
     */
    private void checkLimit()
    {
        Communication communication = super.getContainerCommunicator().collect();
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }

    /**
     * 调用外部hook
     */
    private void invokeHooks()
    {

        String jobKey = "jobName";
        LOG.debug("invokeHooks begin");

        String jobResultReportUrl = userConf.getString(CoreConstant.CORE_SERVER_ADDRESS);
        int requestTimeoutSecs = userConf.getInt(CoreConstant.CORE_SERVER_TIMEOUT_SEC, 2);

        if (StringUtils.isBlank(jobResultReportUrl)) {
            LOG.debug("report url not found");
            return;
        }

        // 获得任务名称，两种方式获取，一种是命令行通过 -DjobName 传递；第二种是分析writer插件的写入路径，提取第2，3个目录拼接
        String jobContentWriterPath = userConf.getString(CoreConstant.JOB_CONTENT_WRITER_PATH);
        StringBuilder jobName = new StringBuilder();
        if (System.getProperty(jobKey) != null) {
            jobName.append(System.getProperty(jobKey));
        }
        else if (StringUtils.isNotBlank(jobContentWriterPath)) {
            String[] pathArr = jobContentWriterPath.split("/");

            if (pathArr.length >= 4) {
                jobName.append(pathArr[2]).append(".").append(pathArr[3]);
            }
        }
        else {
            jobName.append(jobKey);
        }
        LOG.debug("jobName: {}", jobName);

        if (0L == this.endTimeStamp) {
            this.endTimeStamp = System.currentTimeMillis();
        }

        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
        long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;

        Communication communication = super.getContainerCommunicator().collect();

        // 字节速率
        long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES) / transferCosts;
        long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS) / transferCosts;

        Map<String, Object> resultLog = new HashMap<>();

        resultLog.put("startTimeStamp", startTimeStamp / 1000);
        resultLog.put("endTimeStamp", endTimeStamp / 1000);
        resultLog.put("totalCosts", totalCosts);
        resultLog.put("byteSpeedPerSecond", byteSpeedPerSecond);
        resultLog.put("recordSpeedPerSecond", recordSpeedPerSecond);
        resultLog.put("totalReadRecords", CommunicationTool.getTotalReadRecords(communication));
        resultLog.put("totalErrorRecords", CommunicationTool.getTotalErrorRecords(communication));
        resultLog.put("jobName", jobName);
        resultLog.put("jobContent", userConf.getString("jobContent.internal.job"));

        String jsonStr = JSON.toJSONString(resultLog);

        CloseableHttpAsyncClient httpClient = JobReport.getHttpClient(requestTimeoutSecs * 1000);

        LOG.debug("jobResultReportUrl: {}", jobResultReportUrl);
        LOG.debug("report contents: {}", jsonStr);
        HttpPost postBody = JobReport.getPostBody(jobResultReportUrl, jsonStr, ContentType.APPLICATION_JSON);

        //回调
        FutureCallback<HttpResponse> callback = new FutureCallback<HttpResponse>()
        {

            public void completed(HttpResponse result)
            {
                LOG.debug("send jobResult completed, result: {}", result.getStatusLine());
                String content;
                try {
                    content = EntityUtils.toString(result.getEntity(), "UTF-8");
                    LOG.debug("report contents: {}", content);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            public void failed(Exception e)
            {
                e.printStackTrace();
                LOG.warn("send jobResult failed");
            }

            public void cancelled()
            {
                LOG.warn("send jobResult cancelled");
            }
        };
        //连接池执行
        Future<HttpResponse> responseFuture = httpClient.execute(postBody, callback);

        try {
            responseFuture.get();
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        LOG.debug("invokeHooks end");
    }
}
