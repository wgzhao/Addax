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

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.container.CoreConstant;

import java.util.List;
import java.util.Objects;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

/**
 * Typed view over one per-task configuration, i.e. one element of
 * {@code core.container.taskConfigs}.
 * <p>
 * The storage layout is unchanged: a task configuration carries
 * {@code reader.name/parameter}, {@code writer.name/parameter}, the optional
 * {@code transformer} list and {@code taskId}. Reader/writer plugin parameters are
 * passed through untouched; plugins keep receiving the raw configurations.
 * <p>
 * Every accessor delegates to the same {@link Configuration} method the framework used
 * before this view existed, so the copy/live semantics of each read stay exactly the same.
 */
public final class TaskConfig
{
    private final Configuration configuration;

    private TaskConfig(Configuration configuration)
    {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
    }

    /** Wraps an existing task configuration in a typed view. */
    public static TaskConfig of(Configuration taskConfiguration)
    {
        return new TaskConfig(taskConfiguration);
    }

    /**
     * Builds a task configuration. {@code taskId} is the sequential index of the task in
     * the post-split task list; the transformer list is stored only when non-empty.
     */
    public static TaskConfig create(int taskId,
            String readerName, Configuration readerParameter,
            String writerName, Configuration writerParameter,
            List<Configuration> transformers)
    {
        Configuration taskConfiguration = Configuration.newDefault();
        taskConfiguration.set(CoreConstant.JOB_READER_NAME, readerName);
        taskConfiguration.set(CoreConstant.JOB_READER_PARAMETER, readerParameter);
        taskConfiguration.set(CoreConstant.JOB_WRITER_NAME, writerName);
        taskConfiguration.set(CoreConstant.JOB_WRITER_PARAMETER, writerParameter);
        if (transformers != null && !transformers.isEmpty()) {
            taskConfiguration.set(CoreConstant.JOB_TRANSFORMER, transformers);
        }
        taskConfiguration.set(CoreConstant.TASK_ID, taskId);
        return new TaskConfig(taskConfiguration);
    }

    /**
     * The backing configuration for code that still speaks the raw data
     * (storage, cloning, the statistics register boundary).
     */
    public Configuration getConfiguration()
    {
        return configuration;
    }

    /**
     * Deep copy used to isolate retry attempts from each other; mirrors
     * {@link Configuration#clone()}.
     */
    public TaskConfig copy()
    {
        return new TaskConfig(configuration.clone());
    }

    public int getTaskId()
    {
        Integer taskId = configuration.getInt(CoreConstant.TASK_ID);
        if (taskId == null) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The task id is not set: " + CoreConstant.TASK_ID);
        }
        return taskId;
    }

    public String getReaderName()
    {
        return configuration.getString(CoreConstant.JOB_READER_NAME);
    }

    /** Raw reader parameter block for the Reader.Task plugin; null when absent. */
    public Configuration getReaderParameter()
    {
        return configuration.getConfiguration(CoreConstant.JOB_READER_PARAMETER);
    }

    public String getWriterName()
    {
        return configuration.getString(CoreConstant.JOB_WRITER_NAME);
    }

    /** Raw writer parameter block for the Writer.Task plugin; null when absent. */
    public Configuration getWriterParameter()
    {
        return configuration.getConfiguration(CoreConstant.JOB_WRITER_PARAMETER);
    }

    /** The job-level transformer configs copied into this task; empty when none. */
    public List<Configuration> getTransformers()
    {
        return configuration.getListConfiguration(CoreConstant.JOB_TRANSFORMER);
    }

    public boolean hasReader()
    {
        return configuration.getConfiguration(CoreConstant.JOB_READER) != null;
    }

    public boolean hasWriter()
    {
        return configuration.getConfiguration(CoreConstant.JOB_WRITER) != null;
    }

    /**
     * Reader-side load-balance resource mark ({@code reader.parameter.loadBalanceResourceMark}).
     * It is a plugin-facing parameter that the framework also uses to group tasks
     * across task groups.
     */
    public String getReaderResourceMark()
    {
        return configuration.getString(CoreConstant.JOB_READER_PARAMETER + "." + Constant.LOAD_BALANCE_RESOURCE_MARK);
    }

    /** Writer-side load-balance resource mark ({@code writer.parameter.loadBalanceResourceMark}). */
    public String getWriterResourceMark()
    {
        return configuration.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + Constant.LOAD_BALANCE_RESOURCE_MARK);
    }

    /**
     * Plants the framework's placeholder resource mark when the user set none; user jobs
     * set a real mark through the plugin parameters instead.
     */
    public void setReaderResourceMark(String resourceMark)
    {
        configuration.set(CoreConstant.JOB_READER_PARAMETER + "." + Constant.LOAD_BALANCE_RESOURCE_MARK, resourceMark);
    }
}
