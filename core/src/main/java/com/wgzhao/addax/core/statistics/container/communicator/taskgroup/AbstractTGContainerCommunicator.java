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
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.container.collector.ProcessInnerCollector;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;

/**
 * The AbstractTGContainerCommunicator class is the parent class for processing the collection and reporting of taskGroupContainer communication.
 * The main taskCommunicationMap records the communication attributes of taskExecutor.
 */
public abstract class AbstractTGContainerCommunicator
        extends AbstractContainerCommunicator
{
    protected int taskGroupId;

    public AbstractTGContainerCommunicator(Configuration configuration)
    {
        super(configuration);
        super.setCollector(new ProcessInnerCollector());
        this.taskGroupId = configuration.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList)
    {
        super.getCollector().registerTaskCommunication(configurationList);
    }

    @Override
    public final Communication collect()
    {
        return this.getCollector().collectFromTask();
    }

    @Override
    public final State collectState()
    {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication : super.getCollector().getTaskCommunicationMap().values()) {
            communication.mergeStateFrom(taskCommunication);
        }

        return communication.getState();
    }

    @Override
    public final Communication getCommunication(Integer taskId)
    {
        Validate.isTrue(taskId >= 0, "The taskId must not be less than 0");

        return super.getCollector().getTaskCommunication(taskId);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationMap()
    {
        return super.getCollector().getTaskCommunicationMap();
    }
}
