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

package com.wgzhao.addax.common.plugin;

import com.wgzhao.addax.common.base.BaseObject;
import com.wgzhao.addax.common.util.Configuration;

public abstract class AbstractPlugin
        extends BaseObject
        implements Pluginable
{
    //作业的config
    private Configuration pluginJobConf;

    //插件本身的plugin
    private Configuration pluginConf;

    // by qiangsi.lq。 修改为对端的作业configuration
    private Configuration peerPluginJobConf;

    private String peerPluginName;

    @Override
    public String getPluginName()
    {
        assert null != this.pluginConf;
        return this.pluginConf.getString("name");
    }

    @Override
    public String getDeveloper()
    {
        assert null != this.pluginConf;
        return this.pluginConf.getString("developer");
    }

    @Override
    public String getDescription()
    {
        assert null != this.pluginConf;
        return this.pluginConf.getString("description");
    }

    @Override
    public Configuration getPluginJobConf()
    {
        return pluginJobConf;
    }

    @Override
    public void setPluginJobConf(Configuration pluginJobConf)
    {
        this.pluginJobConf = pluginJobConf;
    }

    @Override
    public void setPluginConf(Configuration pluginConf)
    {
        this.pluginConf = pluginConf;
    }

    @Override
    public Configuration getPeerPluginJobConf()
    {
        return peerPluginJobConf;
    }

    @Override
    public void setPeerPluginJobConf(Configuration peerPluginJobConf)
    {
        this.peerPluginJobConf = peerPluginJobConf;
    }

    @Override
    public String getPeerPluginName()
    {
        return peerPluginName;
    }

    @Override
    public void setPeerPluginName(String peerPluginName)
    {
        this.peerPluginName = peerPluginName;
    }

    public void preCheck()
    {
    }

    public void prepare()
    {
    }

    public void post()
    {
    }

    public void preHandler(Configuration jobConfiguration)
    {

    }

    public void postHandler(Configuration jobConfiguration)
    {

    }
}
