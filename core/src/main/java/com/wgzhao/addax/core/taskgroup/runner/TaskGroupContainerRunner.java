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

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.taskgroup.TaskGroupContainer;

import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class TaskGroupContainerRunner
        implements Runnable
{

    private final TaskGroupContainer taskGroupContainer;

    public TaskGroupContainerRunner(TaskGroupContainer taskGroup)
    {
        this.taskGroupContainer = taskGroup;
    }

    @Override
    public void run()
    {
        try {
            Thread.currentThread().setName(String.format("taskGroup-%d", this.taskGroupContainer.getTaskGroupId()));
            this.taskGroupContainer.start();
        }
        catch (Throwable e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }
}
