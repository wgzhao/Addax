package com.wgzhao.datax.core.statistics.container.communicator.taskgroup;

import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.statistics.container.report.ProcessInnerReporter;

public class StandaloneTGContainerCommunicator
        extends AbstractTGContainerCommunicator
{

    public StandaloneTGContainerCommunicator(Configuration configuration)
    {
        super(configuration);
        super.setReporter(new ProcessInnerReporter());
    }

    @Override
    public void report(Communication communication)
    {
        communication.setJobId(super.jobId);//给当前
        super.getReporter().reportTGCommunication(super.taskGroupId, communication);
    }
}
