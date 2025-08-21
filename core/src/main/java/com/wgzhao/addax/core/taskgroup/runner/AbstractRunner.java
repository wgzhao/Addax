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

import com.wgzhao.addax.core.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import org.apache.commons.lang3.Validate;

/**
 * Base runner which wraps a task plugin and manages its lifecycle and communication.
 */
public abstract class AbstractRunner
{
    private AbstractTaskPlugin plugin;

    private Communication runnerCommunication;

    private int taskGroupId;

    private int taskId;

    /**
     * Construct a runner with specified task plugin.
     *
     * @param taskPlugin task plugin to run
     */
    public AbstractRunner(AbstractTaskPlugin taskPlugin)
    {
        this.plugin = taskPlugin;
    }

    /**
     * Destroy the underlying plugin safely.
     */
    public void destroy()
    {
        if (this.plugin != null) {
            this.plugin.destroy();
        }
    }

    /**
     * Get the task plugin instance.
     *
     * @return the plugin
     */
    public AbstractTaskPlugin getPlugin()
    {
        return plugin;
    }

    /**
     * Set the task plugin instance.
     *
     * @param plugin plugin to set
     */
    public void setPlugin(AbstractTaskPlugin plugin)
    {
        this.plugin = plugin;
    }

    /**
     * Set the job configuration for the plugin.
     *
     * @param jobConf configuration
     */
    public void setJobConf(Configuration jobConf)
    {
        this.plugin.setPluginJobConf(jobConf);
    }

    /**
     * Set the TaskPluginCollector for handling dirty data and metrics.
     *
     * @param pluginCollector collector implementation
     */
    public void setTaskPluginCollector(TaskPluginCollector pluginCollector)
    {
        this.plugin.setTaskPluginCollector(pluginCollector);
    }

    private void mark(State state)
    {
        this.runnerCommunication.setState(state);
        if (state == State.SUCCEEDED) {
            // Increase the stage counter on success
            this.runnerCommunication.setLongCounter(CommunicationTool.STAGE,
                    this.runnerCommunication.getLongCounter(CommunicationTool.STAGE) + 1);
        }
    }

    /**
     * Mark the current task as successful.
     */
    public void markSuccess()
    {
        mark(State.SUCCEEDED);
    }

    /**
     * Mark the current task as failed and attach the throwable.
     *
     * @param throwable the cause
     */
    public void markFail(final Throwable throwable)
    {
        mark(State.FAILED);
        this.runnerCommunication.setTimestamp(System.currentTimeMillis());
        this.runnerCommunication.setThrowable(throwable);
    }

    /**
     * Get task group id.
     *
     * @return task group id
     */
    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    /**
     * Set task group id, also propagate to plugin.
     *
     * @param taskGroupId id to set
     */
    public void setTaskGroupId(int taskGroupId)
    {
        this.taskGroupId = taskGroupId;
        this.plugin.setTaskGroupId(taskGroupId);
    }

    /**
     * Get task id.
     *
     * @return task id
     */
    public int getTaskId()
    {
        return taskId;
    }

    /**
     * Set task id, also propagate to plugin.
     *
     * @param taskId id to set
     */
    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
        this.plugin.setTaskId(taskId);
    }

    /**
     * Get the communication for this runner.
     *
     * @return communication
     */
    public Communication getRunnerCommunication()
    {
        return runnerCommunication;
    }

    /**
     * Set the communication for this runner.
     *
     * @param runnerCommunication communication instance
     */
    public void setRunnerCommunication(final Communication runnerCommunication)
    {
        Validate.notNull(runnerCommunication,
                "The Communication of plugin can not be null");
        this.runnerCommunication = runnerCommunication;
    }

    /**
     * Shutdown hook for runner implementations.
     */
    public abstract void shutdown();
}
