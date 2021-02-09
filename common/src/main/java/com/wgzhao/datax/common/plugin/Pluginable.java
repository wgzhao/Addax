package com.wgzhao.datax.common.plugin;

import com.wgzhao.datax.common.util.Configuration;

public interface Pluginable
{
    String getDeveloper();

    String getDescription();

    void setPluginConf(Configuration pluginConf);

    void init();

    void destroy();

    String getPluginName();

    Configuration getPluginJobConf();

    void setPluginJobConf(Configuration jobConf);

    Configuration getPeerPluginJobConf();

    void setPeerPluginJobConf(Configuration peerPluginJobConf);

    String getPeerPluginName();

    void setPeerPluginName(String peerPluginName);
}
