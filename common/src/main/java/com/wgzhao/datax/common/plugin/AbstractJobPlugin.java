package com.wgzhao.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractJobPlugin
        extends AbstractPlugin
{
    private JobPluginCollector jobPluginCollector;

    /**
     * @return the jobPluginCollector
     */
    public JobPluginCollector getJobPluginCollector()
    {
        return jobPluginCollector;
    }

    /**
     * @param jobPluginCollector the jobPluginCollector to set
     */
    public void setJobPluginCollector(
            JobPluginCollector jobPluginCollector)
    {
        this.jobPluginCollector = jobPluginCollector;
    }
}
