/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractTaskPlugin
        extends AbstractPlugin
{

    private int taskGroupId;
    private int taskId;
    private TaskPluginCollector taskPluginCollector;

    public TaskPluginCollector getTaskPluginCollector()
    {
        return taskPluginCollector;
    }

    public void setTaskPluginCollector(
            TaskPluginCollector taskPluginCollector)
    {
        this.taskPluginCollector = taskPluginCollector;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public void setTaskId(int taskId)
    {
        this.taskId = taskId;
    }

    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    public void setTaskGroupId(int taskGroupId)
    {
        this.taskGroupId = taskGroupId;
    }
}
