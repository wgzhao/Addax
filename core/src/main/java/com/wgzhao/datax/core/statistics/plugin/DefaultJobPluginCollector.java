package com.wgzhao.datax.core.statistics.plugin;

import com.wgzhao.datax.common.plugin.JobPluginCollector;
import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.statistics.container.communicator.AbstractContainerCommunicator;

import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-9-9.
 */
public final class DefaultJobPluginCollector
        implements JobPluginCollector
{
    private final AbstractContainerCommunicator jobCollector;

    public DefaultJobPluginCollector(AbstractContainerCommunicator containerCollector)
    {
        this.jobCollector = containerCollector;
    }

    @Override
    public Map<String, List<String>> getMessage()
    {
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage();
    }

    @Override
    public List<String> getMessage(String key)
    {
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage(key);
    }
}
