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

package com.wgzhao.addax.core.statistics.communication;

import com.wgzhao.addax.core.meta.State;
import org.apache.commons.lang3.Validate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalTGCommunicationManager
{
    private static final Map<Integer, Communication> taskGroupCommunicationMap = new ConcurrentHashMap<>();

    private LocalTGCommunicationManager() {}

    public static void registerTaskGroupCommunication(int taskGroupId, Communication communication)
    {
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static Communication getJobCommunication()
    {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskGroupCommunication : taskGroupCommunicationMap.values()) {
            communication.mergeFrom(taskGroupCommunication);
        }

        return communication;
    }

    /**
     * Get the communication of the task group according to the taskGroupId
     * prevent the modification of the map key-value pair
     * @param taskGroupId task group id
     * @return Communication
     */
    public static Communication getTaskGroupCommunication(int taskGroupId)
    {
        Validate.isTrue(taskGroupId >= 0, "The number of taskGroupId cannot be less than zero.");

        return taskGroupCommunicationMap.get(taskGroupId);
    }

    public static void updateTaskGroupCommunication(final int taskGroupId,
            final Communication communication)
    {
        Validate.isTrue(taskGroupCommunicationMap.containsKey(taskGroupId),
                String.format("There is no communication registered for taskGroupId[%d] in taskGroupCommunicationMap," +
                "Unable to update the information for this taskGroup", taskGroupId));
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static void clear()
    {
        taskGroupCommunicationMap.clear();
    }

    public static Map<Integer, Communication> getTaskGroupCommunicationMap()
    {
        return taskGroupCommunicationMap;
    }
}
