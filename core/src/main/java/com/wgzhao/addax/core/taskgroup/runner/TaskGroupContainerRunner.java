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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.taskgroup.TaskGroupContainer;

import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class TaskGroupContainerRunner
        implements Runnable {

    private final TaskGroupContainer taskGroupContainer;

    private State state;

    public TaskGroupContainerRunner(TaskGroupContainer taskGroup) {
        this.taskGroupContainer = taskGroup;
        this.state = State.SUCCEEDED;
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(
                    String.format("taskGroup-%d", this.taskGroupContainer.getTaskGroupId()));
            this.taskGroupContainer.start();
            this.state = State.SUCCEEDED;
        } catch (Throwable e) {
            this.state = State.FAILED;
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e);
        }
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }
}
