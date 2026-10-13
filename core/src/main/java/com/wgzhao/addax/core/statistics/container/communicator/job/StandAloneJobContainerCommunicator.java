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

package com.wgzhao.addax.core.statistics.container.communicator.job;

import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.statistics.communication.LocalTGCommunicationManager;
import com.wgzhao.addax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Job-level communicator: aggregates statistics over the task-group communications
 * kept in {@link LocalTGCommunicationManager}.
 */
public class StandAloneJobContainerCommunicator
        extends AbstractContainerCommunicator
{
    private static final Logger LOG = LoggerFactory.getLogger(StandAloneJobContainerCommunicator.class);

    @Override
    public void registerCommunication(List<Configuration> configurationList)
    {
        for (Configuration config : configurationList) {
            int taskGroupId = config.getInt(CoreConstant.CORE_CONTAINER_TASK_GROUP_ID);
            LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    @Override
    public Communication collect()
    {
        return LocalTGCommunicationManager.getJobCommunication();
    }

    @Override
    public void report(Communication communication)
    {
        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }
}
