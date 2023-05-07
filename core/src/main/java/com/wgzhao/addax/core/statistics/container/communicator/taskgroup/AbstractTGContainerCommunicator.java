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

import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.container.collector.ProcessInnerCollector;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;

/**
 * 该类是用于处理 taskGroupContainer 的 communication 的收集汇报的父类
 * 主要是 taskCommunicationMap 记录了 taskExecutor 的 communication 属性
 */
public abstract class AbstractTGContainerCommunicator
        extends AbstractContainerCommunicator
{
    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     * 所有TG的Collector都是ProcessInnerCollector
     */
    protected int taskGroupId;

    public AbstractTGContainerCommunicator(Configuration configuration)
    {
        super(configuration);
//        this.jobId = configuration.getInt(CoreConstant.CORE_CONTAINER_JOB_ID);
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
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");

        return super.getCollector().getTaskCommunication(taskId);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationMap()
    {
        return super.getCollector().getTaskCommunicationMap();
    }
}
