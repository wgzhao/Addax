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
import com.wgzhao.addax.core.constant.PluginType;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.AbstractJobPlugin;
import com.wgzhao.addax.core.plugin.JobPluginCollector;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.statistics.PerfTrace;
import com.wgzhao.addax.core.statistics.VMInfo;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.StrUtil;
import com.wgzhao.addax.core.AbstractContainer;
import com.wgzhao.addax.core.Engine;
import com.wgzhao.addax.core.container.util.JobAssignUtil;
import com.wgzhao.addax.core.job.scheduler.AbstractScheduler;
import com.wgzhao.addax.core.job.scheduler.processinner.StandAloneScheduler;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.wgzhao.addax.core.statistics.plugin.DefaultJobPluginCollector;
import com.wgzhao.addax.core.util.ErrorRecordChecker;
import com.wgzhao.addax.core.util.container.ClassLoaderSwapper;
import com.wgzhao.addax.core.util.container.CoreConstant;
import com.wgzhao.addax.core.util.container.LoadUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/*
 * Created by jingxing on 14-8-24.
 * <p>
 * The job instance runs in the jobContainer container, which is the master of all tasks.
 * It is responsible for initialization, splitting, scheduling, running, recycling, monitoring and reporting,
 * but it does not perform actual data synchronization operations.
 */
public class JobContainer
        extends AbstractContainer
{
    private static final Logger LOG = LoggerFactory.getLogger(JobContainer.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
    private final ErrorRecordChecker errorLimit;
    private String readerPluginName;
    private String writerPluginName;
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

    /**
     * Start the JobContainer. It manages the whole lifecycle: pre-handle, init, prepare,
     * split, schedule, post, and statistics logging. Supports dry-run pre-check.
     */
    @Override
    public void start()
    {
        LOG.info("The jobContainer begins to process the job.");

        boolean hasException = false;
        boolean isDryRun = false;
        try {
            this.startTimeStamp = System.currentTimeMillis();
            isDryRun = configuration.getBool(CoreConstant.JOB_SETTING_DRY_RUN, false);
            if (isDryRun) {
                LOG.info("The jobContainer begins to perform pre-check ...");
                this.init();
                this.preCheck();
            }
            else {
                userConf = configuration.clone();
                LOG.debug("The jobContainer begins to perform pre-handle ...");
                this.preHandle();

                LOG.debug("The jobContainer begins to perform init ...");
                this.init();
                LOG.debug("The jobContainer begins to perform prepare ...");
                this.prepare();
                LOG.debug("The jobContainer begins to perform split ...");
                this.totalStage = this.split();
                LOG.debug("The jobContainer begins to perform schedule ...");
                this.schedule();
                LOG.debug("The jobContainer begins to perform post ...");
                this.post();

                LOG.debug("The jobContainer begins to perform postHandle ...");
                this.postHandle();

                LOG.debug("The jobContainer completed successfully.");
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
                // Because the containerCollector is initialized in scheduler(),
                // when an exception occurs before scheduler(), containerCollector needs to be initialized here.
                AbstractContainerCommunicator tempContainerCollector;
                // standalone
                tempContainerCollector = new StandAloneJobContainerCommunicator(configuration);
                super.setContainerCommunicator(tempContainerCollector);
            }

            Communication communication = super.getContainerCommunicator().collect();

            communication.setThrowable(e);
            communication.setTimestamp(this.endTimeStamp);

            Communication tempComm = new Communication();
            tempComm.setTimestamp(this.startTransferTimeStamp);

            Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
            super.getContainerCommunicator().report(reportCommunication);

            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e);
        }
        finally {
            if (!isDryRun) {

                this.destroy();
                this.endTimeStamp = System.currentTimeMillis();
                if (!hasException) {
                    VMInfo vmInfo = VMInfo.getVmInfo();
                    if (vmInfo != null) {
                        vmInfo.getDelta(false);
                        LOG.debug(vmInfo.totalString());
                    }
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
        LOG.info("Pre-check passed");
    }

    private void preCheckInit()
    {
        Thread.currentThread().setName("job-0");

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        this.jobReader = this.preCheckReaderInit(jobPluginCollector);
        this.jobWriter = this.preCheckWriterInit(jobPluginCollector);
    }

    private Reader.Job preCheckReaderInit(JobPluginCollector jobPluginCollector)
    {
        this.readerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);

        this.configuration.set(CoreConstant.JOB_CONTENT_READER_PARAMETER + ".dryRun", true);

        // configure the jobConfig of reader
        jobReader.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));
        // use writer parameters as peer for reader during pre-check
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));
        jobReader.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    private Writer.Job preCheckWriterInit(JobPluginCollector jobPluginCollector)
    {
        this.writerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);

        this.configuration.set(CoreConstant.JOB_CONTENT_WRITER_PARAMETER + ".dryRun", true);

        // set job config for writer
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));
        // set peer config (reader) for writer
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void preCheckReader()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        LOG.info("The Reader.Job [{}] perform pre-check work .", this.readerPluginName);
        this.jobReader.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        LOG.info("The Writer.Job [{}] perform pre-check work .", this.writerPluginName);
        this.jobWriter.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /*
     * Initialize reader and writer plugins
     */
    private void init()
    {
        Thread.currentThread().setName("job-0");

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        // reader must be initialized before writer
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
                    CONFIG_ERROR,
                    String.format("The plugin type (%s) set for the pre-handler of job failed, reason: %s", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(
                CoreConstant.JOB_PRE_HANDLER_PLUGIN_NAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);

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
                    CONFIG_ERROR,
                    String.format("The plugin type (%s) set for the post-handler of job failed, reason: %s", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(CoreConstant.JOB_POST_HANDLER_PLUGIN_NAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        handler.postHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /*
     * Perform the finest-grained split for reader and writer. Writer's split should match
     * reader's split count to satisfy the 1:1 channel model. Then merge the parameters into
     * content and optionally shuffle to avoid long-tail effects.
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

        LOG.debug("The transformer configuration:{} ", JSON.toJSONString(transformerList));
        /*
         * input is reader and writer parameter list, output is content list
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(readerTaskConfigs, writerTaskConfigs, transformerList);

        LOG.debug("The contentConfig configuration:{} ", JSON.toJSONString(contentConfig));

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

            // Under byte-rate limit, the per-channel byte limit must be set, otherwise fail
            Long channelLimitedByteSpeed = this.configuration.getLong(CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_BYTE, -1);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                throw AddaxException.asAddaxException(
                        CONFIG_ERROR,
                        "Under the condition of total bps limit, the bps value of a single channel cannot be empty or non-positive");
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
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Under the condition of total tps limit, the tps value of a single channel cannot be empty or non-positive");
            }

            needChannelNumberByRecord = (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to {} records.", globalLimitedRecordSpeed);
        }

        this.needChannelNumber = Math.min(needChannelNumberByByte, needChannelNumberByRecord);

        // if set this value via byte or record ,then skip
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
     * schedule merges the split results into TaskGroupContainer and dispatches them
     * according to the execution mode.
     */
    private void schedule()
    {
        /*
         * The global and per-channel speed are in B/s
         */
        int channelsPerTaskGroup = this.configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_CHANNEL, 5);
        int taskNumber = this.configuration.getList(CoreConstant.JOB_CONTENT).size();

        this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
        PerfTrace.getInstance().setChannelNumber(needChannelNumber);

        /*
         * Get which tasks each taskGroup should run from configuration
         */

        List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.configuration, this.needChannelNumber, channelsPerTaskGroup);

        LOG.info("The Scheduler launches [{}] taskGroup(s).", taskGroupConfigs.size());

        AbstractScheduler scheduler;
        try {
            scheduler = initStandaloneScheduler(this.configuration);
            this.startTransferTimeStamp = System.currentTimeMillis();
            scheduler.schedule(taskGroupConfigs);
            this.endTransferTimeStamp = System.currentTimeMillis();
        }
        catch (Exception e) {
            LOG.error("The scheduler failed to run.");
            this.endTransferTimeStamp = System.currentTimeMillis();
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e);
        }

        /*
         * Check job limits
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

        // byte speed
        long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES) / transferCosts;
        long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS) / transferCosts;

        long totalReadRecords = CommunicationTool.getTotalReadRecords(communication);
        long totalErrorRecords = CommunicationTool.getTotalErrorRecords(communication);

        reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
        reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

        super.getContainerCommunicator().report(reportCommunication);
        LOG.debug(CommunicationTool.Stringify.getSnapshot(communication));

        //try to report the job statistic to report server if present
        String jobResultReportUrl = userConf.getString(CoreConstant.CORE_SERVER_ADDRESS);
        if (StringUtils.isNotBlank(jobResultReportUrl)) {
            String jobKey = "jobName";
            // Get the job name, there are two ways to get it:
            // 1. Pass it through the command line using -DjobName;
            // 2. Analyze the log writing path of the writer plugin and splice the 2nd and 3rd directories
            String jobContentWriterPath = userConf.getString(CoreConstant.JOB_CONTENT_WRITER_PATH);
            int timeoutMills = userConf.getInt(CoreConstant.CORE_SERVER_TIMEOUT_SEC, 2) * 1000;
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

            Map<String, Object> resultLog = new HashMap<>();

            resultLog.put("startTimeStamp", startTimeStamp / 1000);
            resultLog.put("endTimeStamp", endTimeStamp / 1000);
            resultLog.put("totalCosts", totalCosts);
            resultLog.put("byteSpeedPerSecond", byteSpeedPerSecond);
            resultLog.put("recordSpeedPerSecond", recordSpeedPerSecond);
            resultLog.put("totalReadRecords", totalReadRecords);
            resultLog.put("totalErrorRecords", totalErrorRecords);
            resultLog.put("jobName", jobName);
            resultLog.put("jobContent", userConf.getString("jobContent.internal.job"));

            String jsonStr = JSON.toJSONString(resultLog);

            LOG.info("The jobResultReportUrl: {}", jobResultReportUrl);
            LOG.debug("The report contents: {}", jsonStr);
            postJobRunStatistic(jobResultReportUrl, timeoutMills, jsonStr);
        }
        String statMsg = String.format("%n" + "%-26s: %-18s%n" + "%-26s: %-18s%n" + "%-26s: %19s%n"
                        + "%-26s: %19s%n" + "%-26s: %19s%n" + "%-26s: %19s%n" + "%-26s: %19s%n",
                "Job start  at", dateFormat.format(startTimeStamp),
                "Job end    at", dateFormat.format(endTimeStamp),
                "Job took secs", totalCosts + "s",
                "Average   bps", StrUtil.stringify(byteSpeedPerSecond) + "/s",
                "Average   rps", recordSpeedPerSecond + "rec/s",
                "Number of rec", totalReadRecords,
                "Failed record", totalErrorRecords
        );
        LOG.info(statMsg);
        final Long counterSucc = communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS);
        final Long counterFail = communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS);
        final Long counterFilter = communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS);
        if (counterSucc + counterFail + counterFilter > 0) {
            String transStatMsg = String.format("%n" + "%-26s: %19s%n" + "%-26s: %19s%n" + "%-26s: %19s%n",
                    "Transformer success records", counterSucc,
                    "Transformer failed  records", counterFail,
                    "Transformer filter  records", counterFilter
            );
            LOG.info(transStatMsg);
        }
    }

    /*
     * Initialize reader job and return Reader.Job
     */
    private Reader.Job initJobReader(
            JobPluginCollector jobPluginCollector)
    {
        this.readerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);
        // set job config for reader
        jobReader.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));

        // set peer config (writer) for reader
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));

        jobReader.setJobPluginCollector(jobPluginCollector);
        jobReader.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    /*
     * Initialize writer job and return Writer.Job
     */
    private Writer.Job initJobWriter(JobPluginCollector jobPluginCollector)
    {
        this.writerPluginName = this.configuration.getString(CoreConstant.JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);
        // set job config for writer
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER));

        // set peer config (reader) for writer
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void prepareJobReader()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        LOG.info("The Reader.Job [{}] perform prepare work .", this.readerPluginName);
        this.jobReader.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        LOG.info("The Writer.Job [{}] perform prepare work .", this.writerPluginName);
        this.jobWriter.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private List<Configuration> doReaderSplit(int adviceNumber)
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        List<Configuration> readerSlicesConfigs = this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.isEmpty()) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "The number of tasks divided by the reader's job cannot be less than or equal to zero");
        }
        LOG.info("The Reader.Job [{}] is divided into [{}] task(s).", this.readerPluginName, readerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber)
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));

        List<Configuration> writerSlicesConfigs = this.jobWriter.split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.isEmpty()) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "The number of tasks divided by the writer's job cannot be less than or equal to zero");
        }
        LOG.info("The Writer.Job [{}] is divided into [{}] task(s).", this.writerPluginName, writerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    /*
     * Merge reader and writer task configs into full task content by index order.
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs,
            List<Configuration> transformerConfigs)
    {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    "Number of reader tasks (%d) does not match writer tasks (%d)"
                            .formatted(readerTasksConfigs.size(), writerTasksConfigs.size())
            );
        }

        List<Configuration> contentConfigs = new ArrayList<>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            taskConfig.set(CoreConstant.JOB_READER_NAME, this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER, readerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_WRITER_NAME, this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER, writerTasksConfigs.get(i));

            // Avoid NPE if transformer list is null
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
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        LOG.info("The Reader.Job [{}] perform post work.", this.readerPluginName);
        this.jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter()
    {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        LOG.info("The Writer.Job [{}] perform post work.", this.writerPluginName);
        this.jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * Check whether the final results exceed thresholds. If threshold < 1, it's a percentage; otherwise it's a record count.
     */
    private void checkLimit()
    {
        Communication communication = super.getContainerCommunicator().collect();
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }

    // post job run statistic to server if present
    private void postJobRunStatistic(String url, int timeoutMills, String jsonStr)
    {
        LOG.info("Upload the job run statistics to [{}]", url);

        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofMillis(timeoutMills))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(java.time.Duration.ofMillis(timeoutMills))
                .POST(HttpRequest.BodyPublishers.ofString(jsonStr))
                .build();

        try {
            HttpResponse<String> httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (httpResponse.statusCode() == 200) {
                LOG.info("Job results uploaded successfully");
            } else {
                LOG.warn("Failed to upload job results, the response code: {}", httpResponse.statusCode());
            }
        }
        catch (IOException | InterruptedException e) {
            // Preserve interrupt status when interrupted during HTTP reporting
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn("Exception occurred while uploading the job results: {}", e.getMessage());
        }
    }

    /**
     * Invoke external hooks if any (currently noop).
     */
    private void invokeHooks()
    {
        // no-op
    }
}
