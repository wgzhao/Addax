package com.wgzhao.datax.core.statistics.container.communicator.job;

import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.meta.State;
import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.statistics.communication.CommunicationTool;
import com.wgzhao.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.wgzhao.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.datax.core.statistics.container.report.ProcessInnerReporter;
import com.wgzhao.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class StandAloneJobContainerCommunicator
        extends AbstractContainerCommunicator
{
    private static final Logger LOG = LoggerFactory
            .getLogger(StandAloneJobContainerCommunicator.class);

    public StandAloneJobContainerCommunicator(Configuration configuration)
    {
        super(configuration);
        setCollector(new ProcessInnerCollector(configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        setReporter(new ProcessInnerReporter());
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList)
    {
        getCollector().registerTGCommunication(configurationList);
    }

    @Override
    public Communication collect()
    {
        return getCollector().collectFromTaskGroup();
    }

    @Override
    public State collectState()
    {
        return this.collect().getState();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication)
    {
        getReporter().reportJobCommunication(getJobId(), communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }

    @Override
    public Communication getCommunication(Integer taskGroupId)
    {
        return getCollector().getTGCommunication(taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap()
    {
        return getCollector().getTGCommunicationMap();
    }
}
