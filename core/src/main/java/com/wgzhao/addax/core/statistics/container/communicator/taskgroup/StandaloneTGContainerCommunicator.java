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

package com.wgzhao.addax.core.statistics.container.communicator.taskgroup;

import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.meta.TaskState;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.LocalTGCommunicationManager;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Task-group communicator: keeps the communication of each taskExecutor in this
 * task group and publishes the group result to {@link LocalTGCommunicationManager}.
 */
public class StandaloneTGContainerCommunicator
        extends AbstractContainerCommunicator
{
    private final int taskGroupId;
    private final Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<>();

    public StandaloneTGContainerCommunicator(Configuration configuration)
    {
        this.taskGroupId = configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList)
    {
        for (Configuration taskConfig : configurationList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            this.taskCommunicationMap.put(taskId, new Communication());
        }
    }

    @Override
    public Communication collect()
    {
        Communication communication = Communication.newAggregate();

        for (Communication taskCommunication : this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    /**
     * The state of the group alone, without merging the counters: this runs on every
     * tick of the task-group loop.
     */
    public State collectState()
    {
        TaskState aggregate = TaskState.EMPTY;

        for (Communication taskCommunication : this.taskCommunicationMap.values()) {
            aggregate = aggregate.merge(taskCommunication.getTaskState());
        }

        return aggregate.getStatus();
    }

    public Communication getCommunication(Integer taskId)
    {
        Validate.isTrue(taskId >= 0, "The taskId must not be less than 0");

        return this.taskCommunicationMap.get(taskId);
    }

    public Map<Integer, Communication> getCommunicationMap()
    {
        return this.taskCommunicationMap;
    }

    public void resetCommunication(Integer taskId)
    {
        this.taskCommunicationMap.put(taskId, new Communication());
    }

    @Override
    public void report(Communication communication)
    {
        LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
    }
}
