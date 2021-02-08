package com.wgzhao.datax.core.statistics.container.communicator;

import com.wgzhao.datax.common.statistics.VMInfo;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.meta.State;
import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.statistics.container.collector.AbstractCollector;
import com.wgzhao.datax.core.statistics.container.report.AbstractReporter;
import com.wgzhao.datax.core.util.container.CoreConstant;

import java.util.List;
import java.util.Map;

public abstract class AbstractContainerCommunicator
{
    private final Configuration configuration;
    private final Long jobId;
    private final VMInfo vmInfo = VMInfo.getVmInfo();
    private AbstractCollector collector;
    private AbstractReporter reporter;
    private long lastReportTime = System.currentTimeMillis();

    public AbstractContainerCommunicator(Configuration configuration)
    {
        this.configuration = configuration;
        this.jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    }

    public Configuration getConfiguration()
    {
        return this.configuration;
    }

    public AbstractCollector getCollector()
    {
        return collector;
    }

    public void setCollector(AbstractCollector collector)
    {
        this.collector = collector;
    }

    public AbstractReporter getReporter()
    {
        return reporter;
    }

    public void setReporter(AbstractReporter reporter)
    {
        this.reporter = reporter;
    }

    public Long getJobId()
    {
        return jobId;
    }

    public abstract void registerCommunication(List<Configuration> configurationList);

    public abstract Communication collect();

    public abstract void report(Communication communication);

    public abstract State collectState();

    public abstract Communication getCommunication(Integer id);

    /**
     * 当 实现是 TGContainerCommunicator 时，返回的 Map: key=taskId, value=Communication
     * 当 实现是 JobContainerCommunicator 时，返回的 Map: key=taskGroupId, value=Communication
     */
    public abstract Map<Integer, Communication> getCommunicationMap();

    public void resetCommunication(Integer id)
    {
        Map<Integer, Communication> map = getCommunicationMap();
        map.put(id, new Communication());
    }

    public void reportVmInfo()
    {
        long now = System.currentTimeMillis();
        //每5分钟打印一次
        if (now - lastReportTime >= 300000) {
            //当前仅打印
            if (vmInfo != null) {
                vmInfo.getDelta(true);
            }
            lastReportTime = now;
        }
    }
}