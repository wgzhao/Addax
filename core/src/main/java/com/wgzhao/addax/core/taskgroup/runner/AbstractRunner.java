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

package com.wgzhao.addax.core.taskgroup.runner;

import com.wgzhao.addax.common.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import org.apache.commons.lang3.Validate;

public abstract class AbstractRunner
{
    private AbstractTaskPlugin plugin;

    private Configuration jobConf;

    private Communication runnerCommunication;

    private int taskGroupId;

    private int taskId;

    public AbstractRunner(AbstractTaskPlugin taskPlugin)
    {
        this.plugin = taskPlugin;
    }

    public void destroy()
    {
        if (this.plugin != null) {
            this.plugin.destroy();
        }
    }

    public State getRunnerState()
    {
        return this.runnerCommunication.getState();
    }

    public AbstractTaskPlugin getPlugin()
    {
        return plugin;
    }

    public void setPlugin(AbstractTaskPlugin plugin)
    {
        this.plugin = plugin;
    }

    public Configuration getJobConf()
    {
        return jobConf;
    }

    public void setJobConf(Configuration jobConf)
    {
        this.jobConf = jobConf;
        this.plugin.setPluginJobConf(jobConf);
    }

    public void setTaskPluginCollector(TaskPluginCollector pluginCollector)
    {
        this.plugin.setTaskPluginCollector(pluginCollector);
    }

    private void mark(State state)
    {
        this.runnerCommunication.setState(state);
        if (state == State.SUCCEEDED) {
            // 对 stage + 1
            this.runnerCommunication.setLongCounter(CommunicationTool.STAGE,
                    this.runnerCommunication.getLongCounter(CommunicationTool.STAGE) + 1);
        }
    }

    public void markRun()
    {
        mark(State.RUNNING);
    }

    public void markSuccess()
    {
        mark(State.SUCCEEDED);
    }

    public void markFail(final Throwable throwable)
    {
        mark(State.FAILED);
        this.runnerCommunication.setTimestamp(System.currentTimeMillis());
        this.runnerCommunication.setThrowable(throwable);
    }

    /**
     * @return the taskGroupId
     */
    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    /**
     * @param taskGroupId the taskGroupId to set
     */
    public void setTaskGroupId(int taskGroupId)
    {
        this.taskGroupId = taskGroupId;
        this.plugin.setTaskGroupId(taskGroupId);
    }

    public int getTaskId()
    {
        return taskId;
    }

    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
        this.plugin.setTaskId(taskId);
    }

    public Communication getRunnerCommunication()
    {
        return runnerCommunication;
    }

    public void setRunnerCommunication(final Communication runnerCommunication)
    {
        Validate.notNull(runnerCommunication,
                "插件的Communication不能为空");
        this.runnerCommunication = runnerCommunication;
    }

    public abstract void shutdown();
}
