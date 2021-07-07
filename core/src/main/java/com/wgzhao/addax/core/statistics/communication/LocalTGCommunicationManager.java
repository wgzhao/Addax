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
    private static final Map<Integer, Communication> taskGroupCommunicationMap =
            new ConcurrentHashMap<>();

    private LocalTGCommunicationManager() {}

    public static void registerTaskGroupCommunication(
            int taskGroupId, Communication communication)
    {
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static Communication getJobCommunication(Long jobId)
    {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskGroupCommunication :
                taskGroupCommunicationMap.values()) {
            if (taskGroupCommunication.getJobId() == null) {
                communication.mergeFrom(taskGroupCommunication);
            }
            if (taskGroupCommunication.getJobId() == null
                    || jobId.equals(taskGroupCommunication.getJobId())) { //如JOB在正式启动后过段时间才会设置JobId所以这里把getJobId为空的也合并进去
                communication.mergeFrom(taskGroupCommunication);        //因为如果为空就说明里面啥都没有合并了也不会影响什么
            }
        }

        return communication;
    }

    /**
     * 采用获取taskGroupId后再获取对应communication的方式，
     * 防止map遍历时修改，同时也防止对map key-value对的修改
     *
     * @param taskGroupId task group id
     * @return set
     */
    public static Communication getTaskGroupCommunication(int taskGroupId)
    {
        Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");

        return taskGroupCommunicationMap.get(taskGroupId);
    }

    public static void updateTaskGroupCommunication(final int taskGroupId,
            final Communication communication)
    {
        Validate.isTrue(taskGroupCommunicationMap.containsKey(
                taskGroupId), String.format("taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" +
                "无法更新该taskGroup的信息", taskGroupId));
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