package com.wgzhao.datax.core.job.scheduler.processinner;

import com.wgzhao.datax.core.statistics.container.communicator.AbstractContainerCommunicator;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class StandAloneScheduler
        extends ProcessInnerScheduler
{

    public StandAloneScheduler(AbstractContainerCommunicator containerCommunicator)
    {
        super(containerCommunicator);
    }

    @Override
    protected boolean isJobKilling(Long jobId)
    {
        return false;
    }
}
