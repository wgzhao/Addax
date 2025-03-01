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

import com.wgzhao.addax.common.element.Record;

/**
 * TaskPluginCollector is used to record dirty records and custom information for Task Plugin. <br >
 * <p>
 * 1. Dirty data records, TaskPluginCollector provides a variety of dirty data record adapters, including local output, centralized reporting, etc. <br >
 * 2. Custom information, all task plugins can collect information through TaskPluginCollector during the running process, <br >
 * Job plugins can get information through getMessage() interface in POST process
 */
public abstract class TaskPluginCollector
        implements PluginCollector
{

    /**
     * Collect dirty data
     *
     * @param dirtyRecord dirty data information
     * @param t exception information
     * @param errorMessage error message
     */
    public abstract void collectDirtyRecord(Record dirtyRecord,
            Throwable t, String errorMessage);

    /**
     * Collect dirty data
     *
     * @param dirtyRecord dirty data information
     * @param errorMessage error message
     */
    public void collectDirtyRecord(Record dirtyRecord,
            String errorMessage)
    {
        this.collectDirtyRecord(dirtyRecord, null, errorMessage);
    }

    /**
     * Collect dirty data
     *
     * @param dirtyRecord dirty data information
     * @param t exception information
     */
    public void collectDirtyRecord(Record dirtyRecord, Throwable t)
    {
        this.collectDirtyRecord(dirtyRecord, t, "");
    }

    /**
     * Collect custom information, Job plugins can get this information through getMessage <br >
     * If multiple keys conflict, use List to record the same key, multiple value situations. <br >
     *
     * @param key message key
     * @param value message content
     */
    public abstract void collectMessage(String key, String value);
}
