package com.wgzhao.datax.core.statistics.container.report;

import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerReporter
        extends AbstractReporter
{

    @Override
    public void reportJobCommunication(Long jobId, Communication communication)
    {
        // do nothing
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication)
    {
        LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
    }
}