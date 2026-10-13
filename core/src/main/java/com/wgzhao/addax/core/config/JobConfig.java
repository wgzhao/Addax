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

package com.wgzhao.addax.core.config;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.statistics.plugin.task.StdoutPluginCollector;
import com.wgzhao.addax.core.transport.channel.memory.MemoryChannel;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.container.CoreConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

/**
 * Typed view over the framework-owned keys of a job-level configuration.
 * <p>
 * A task-group configuration is a deep clone of the job configuration plus per-group
 * overrides, so the same view serves both levels; callers are responsible for only
 * reading keys that exist at their level.
 * <p>
 * Every accessor delegates to the same {@link Configuration} method the framework used
 * before this view existed, so the storage layout and the copy/live semantics of each
 * read stay exactly the same. Reader/writer plugin parameters are never rewritten by
 * this view; plugins keep receiving the raw configurations.
 */
public final class JobConfig
{
    private final Configuration configuration;

    public JobConfig(Configuration configuration)
    {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
    }

    /**
     * The backing configuration for code that still speaks the raw data
     * (storage, cloning, plugin wiring).
     */
    public Configuration getConfiguration()
    {
        return configuration;
    }

    /**
     * Deep copy wrapped in a new view; mirrors {@link Configuration#clone()}, which is
     * used to derive each task-group configuration from the job configuration.
     */
    public JobConfig copy()
    {
        return new JobConfig(configuration.clone());
    }

    // ---------------------------------------------------------------- job.setting

    public boolean isDryRun()
    {
        return configuration.getBool(CoreConstant.JOB_SETTING_DRY_RUN, false);
    }

    /**
     * Marks the job as dry-run so both reader and writer plugin parameters carry the flag
     * the plugins read during their own pre-check.
     */
    public void markDryRun()
    {
        configuration.set(CoreConstant.JOB_CONTENT_READER_PARAMETER + "." + Key.DRY_RUN, true);
        configuration.set(CoreConstant.JOB_CONTENT_WRITER_PARAMETER + "." + Key.DRY_RUN, true);
    }

    /** Total byte speed limit in B/s, null when unset. */
    public Long getSpeedByte()
    {
        return configuration.getLong(CoreConstant.JOB_SETTING_SPEED_BYTE);
    }

    /** Total record speed limit in records/s, null when unset. */
    public Long getSpeedRecord()
    {
        return configuration.getLong(CoreConstant.JOB_SETTING_SPEED_RECORD);
    }

    /** Channel number used when no byte/record speed limit is configured; defaults to 1. */
    public int getSpeedChannel()
    {
        return configuration.getInt(CoreConstant.JOB_SETTING_SPEED_CHANNEL, 1);
    }

    /** Dirty-data record limit, null when unset. */
    public Long getErrorLimitRecord()
    {
        return configuration.getLong(CoreConstant.JOB_SETTING_ERROR_LIMIT_RECORD);
    }

    /** Dirty-data percentage limit, null when unset. */
    public Double getErrorLimitPercentage()
    {
        return configuration.getDouble(CoreConstant.JOB_SETTING_ERROR_LIMIT_PERCENTAGE);
    }

    // ---------------------------------------------------------------- job.content

    /** The job-level transformer list; empty when none configured. */
    public List<Configuration> getTransformers()
    {
        return configuration.getListConfiguration(CoreConstant.JOB_CONTENT_TRANSFORMER);
    }

    public String getReaderName()
    {
        return configuration.getString(CoreConstant.JOB_CONTENT_READER_NAME);
    }

    /** The reader plugin parameter block (job.content.reader.parameter). */
    public Configuration getReaderParameter()
    {
        return configuration.getConfiguration(CoreConstant.JOB_CONTENT_READER_PARAMETER);
    }

    public String getWriterName()
    {
        return configuration.getString(CoreConstant.JOB_CONTENT_WRITER_NAME);
    }

    /** The writer plugin parameter block (job.content.writer.parameter). */
    public Configuration getWriterParameter()
    {
        return configuration.getConfiguration(CoreConstant.JOB_CONTENT_WRITER_PARAMETER);
    }

    /** The writer output path (job.content.writer.parameter.path), used to derive the job name. */
    public String getWriterPath()
    {
        return configuration.getString(CoreConstant.JOB_CONTENT_WRITER_PATH);
    }

    // ---------------------------------------------------------------- core.container

    /**
     * Typed views over the post-split task list; empty when absent. Each element is a
     * view over a copy of the stored task configuration, mirroring
     * {@link Configuration#getListConfiguration(String)}.
     */
    public List<TaskConfig> getTaskConfigs()
    {
        List<Configuration> taskConfigurations =
                configuration.getListConfiguration(CoreConstant.CORE_CONTAINER_TASK_CONFIGS);
        List<TaskConfig> taskConfigs = new ArrayList<>(taskConfigurations.size());
        for (Configuration taskConfiguration : taskConfigurations) {
            taskConfigs.add(TaskConfig.of(taskConfiguration));
        }
        return taskConfigs;
    }

    /** Stores the post-split task list; only the backing task configurations are persisted. */
    public void setTaskConfigs(List<TaskConfig> taskConfigs)
    {
        configuration.set(CoreConstant.CORE_CONTAINER_TASK_CONFIGS,
                taskConfigs.stream().map(TaskConfig::getConfiguration).toList());
    }

    /** Removes the task list; throws when the key is absent, mirroring {@link Configuration#remove(String)}. */
    public void removeTaskConfigs()
    {
        configuration.remove(CoreConstant.CORE_CONTAINER_TASK_CONFIGS);
    }

    public int getTaskGroupId()
    {
        Integer taskGroupId = configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_ID);
        if (taskGroupId == null) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The task group id is not set: " + CoreConstant.CORE_CONTAINER_TASK_GROUP_ID);
        }
        return taskGroupId;
    }

    public void setTaskGroupId(int taskGroupId)
    {
        configuration.set(CoreConstant.CORE_CONTAINER_TASK_GROUP_ID, taskGroupId);
    }

    /** Channels of this task group; null when unset (callers apply their own default). */
    public Integer getTaskGroupChannel()
    {
        return configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_CHANNEL);
    }

    public void setTaskGroupChannel(int channels)
    {
        configuration.set(CoreConstant.CORE_CONTAINER_TASK_GROUP_CHANNEL, channels);
    }

    public int getTaskGroupSleepInterval()
    {
        return configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_SLEEP_INTERVAL, 100);
    }

    public long getTaskGroupReportInterval()
    {
        return configuration.getLong(CoreConstant.CORE_CONTAINER_TASK_GROUP_REPORT_INTERVAL, 10000);
    }

    public int getJobReportInterval()
    {
        return configuration.getInt(CoreConstant.CORE_CONTAINER_JOB_REPORT_INTERVAL, 30000);
    }

    public int getJobSleepInterval()
    {
        return configuration.getInt(CoreConstant.CORE_CONTAINER_JOB_SLEEP_INTERVAL, 10000);
    }

    public int getFailOverMaxRetryTimes()
    {
        return configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_FAIL_OVER_MAX_RETRY_TIMES, 1);
    }

    public long getFailOverRetryIntervalMs()
    {
        return configuration.getLong(CoreConstant.CORE_CONTAINER_TASK_FAIL_OVER_RETRY_INTERVAL_IN_MSEC, 10000);
    }

    public long getFailOverMaxWaitMs()
    {
        return configuration.getLong(CoreConstant.CORE_CONTAINER_TASK_FAIL_OVER_MAX_WAIT_IN_MSEC, 60000);
    }

    // ---------------------------------------------------------------- core.transport

    public String getChannelClass()
    {
        return configuration.getString(CoreConstant.CORE_TRANSPORT_CHANNEL_CLASS, MemoryChannel.class.getName());
    }

    /** Per-channel byte speed limit in B/s, null when unset. */
    public Long getChannelSpeedByte()
    {
        return configuration.getLong(CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
    }

    /** Per-channel record speed limit in records/s, null when unset. */
    public Long getChannelSpeedRecord()
    {
        return configuration.getLong(CoreConstant.CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
    }

    // ---------------------------------------------------------------- core.statistics / core.server

    public String getTaskCollectorClass()
    {
        return configuration.getString(CoreConstant.CORE_STATISTICS_COLLECTOR_PLUGIN_TASK_CLASS,
                StdoutPluginCollector.class.getName());
    }

    /** Result-report server address; blank disables result reporting. */
    public String getServerAddress()
    {
        return configuration.getString(CoreConstant.CORE_SERVER_ADDRESS);
    }

    /** Result-report timeout in seconds; callers convert to milliseconds. */
    public int getServerTimeoutSec()
    {
        return configuration.getInt(CoreConstant.CORE_SERVER_TIMEOUT_SEC, 2);
    }

    // ---------------------------------------------------------------- job.preHandler / job.postHandler

    public String getPreHandlerPluginType()
    {
        return configuration.getString(CoreConstant.JOB_PRE_HANDLER_PLUGIN_TYPE);
    }

    public String getPreHandlerPluginName()
    {
        return configuration.getString(CoreConstant.JOB_PRE_HANDLER_PLUGIN_NAME);
    }

    public String getPostHandlerPluginType()
    {
        return configuration.getString(CoreConstant.JOB_POST_HANDLER_PLUGIN_TYPE);
    }

    public String getPostHandlerPluginName()
    {
        return configuration.getString(CoreConstant.JOB_POST_HANDLER_PLUGIN_NAME);
    }
}
