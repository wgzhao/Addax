package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import jdk.nashorn.internal.runtime.ParserException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine
{
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    /* check job model (job/task) first */
    public void start(Configuration allConf)
    {

        // 绑定column转换信息
        ColumnCast.bind(allConf);

        /*
         * 初始化PluginLoader，可以获取各种插件配置
         */
        LoadUtil.bind(allConf);

        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf
                .getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));
        //JobContainer会在schedule后再行进行设置和调整值
        int channelNumber = 0;
        AbstractContainer container;
        long instanceId;
        int taskGroupId = -1;
        if (isJob) {
            allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, "standalone");
            container = new JobContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
        }
        else {
            container = new TaskGroupContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
            taskGroupId = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            channelNumber = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
        }

        //缺省打开perfTrace
        boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE,
                false);
        boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG,
                false);

        //standlone模式的datax shell任务不进行汇报
        if (instanceId == -1) {
            perfReportEnable = false;
        }

        int priority = 0;

        Configuration jobInfoConfig = allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);
        //初始化PerfTrace
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig, perfReportEnable, channelNumber);
        container.start();
    }

    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration)
    {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration)
    {
        Set<String> keys = configuration.getKeys();
        for (String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, "*****");
            }
        }
        return configuration;
    }

    public static void entry(String[] args)
    {
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");

        try {
            DefaultParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args);

            String jobPath = cl.getOptionValue("job");

            Configuration configuration = ConfigParser.parse(jobPath);
            // jobid 默认值为-1
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);
            // 默认运行模式
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, "standalone");

            //打印vmInfo
            VMInfo vmInfo = VMInfo.getVmInfo();
            if (vmInfo != null) {
                LOG.info(vmInfo.toString());
            }

            LOG.info("\n{}\n", Engine.filterJobConfiguration(configuration));

            LOG.debug(configuration.toJSON());

            ConfigurationValidate.doValidate(configuration);
            Engine engine = new Engine();
            engine.start(configuration);
        }
        catch (ParseException e) {
            throw new ParserException(e.getMessage());
        }
        catch (DataXException e) {
            throw new DataXException(e.getErrorCode(), e.getMessage());
        }
    }

    public static void main(String[] args)
    {
        int exitCode = 0;
        if (args.length < 2) {
            LOG.error("need a job file");
            System.exit(1);
        }
        try {
            Engine.entry(args);
        }
        catch (DataXException e) {
            ErrorCode errorCode = e.getErrorCode();
            LOG.error(e.getMessage());
            if (errorCode instanceof FrameworkErrorCode) {
                FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                exitCode = tempErrorCode.toExitValue();
                System.exit(exitCode);
            } else {
                System.exit(99);
            }
        }
        System.exit(exitCode);
    }
}
