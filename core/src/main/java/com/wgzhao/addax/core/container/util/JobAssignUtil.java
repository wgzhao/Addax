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

package com.wgzhao.addax.core.container.util;

import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.StringUtils;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.base.Constant.LOAD_BALANCE_RESOURCE_MARK;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_CONTAINER_TASK_GROUP_CHANNEL;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_CONTAINER_TASK_GROUP_ID;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_READER_PARAMETER;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_WRITER_PARAMETER;

public final class JobAssignUtil
{
    private JobAssignUtil()
    {
    }

    /**
     * Assign tasks to task groups fairly.
     * Fairness is reflected in the more balanced job distribution based on the load identification of the resource load in the task.
     *
     * @param configuration configuration
     * @param channelNumber the number of channel
     * @param channelsPerTaskGroup the channel of task group
     * @return list of configuration
     */
    public static List<Configuration> assignFairly(Configuration configuration, int channelNumber, int channelsPerTaskGroup)
    {
        Validate.isTrue(configuration != null, "The `job.content` can not be null.");

        List<Configuration> contentConfig = configuration.getListConfiguration(JOB_CONTENT);
        Validate.isTrue(!contentConfig.isEmpty(), "The `job.content` is empty");

        Validate.isTrue(channelNumber > 0 && channelsPerTaskGroup > 0,
                "The average number of tasks per channel [averTaskPerChannel], the number of channels [channelNumber], and the average number of channels per task group " +
                        "[channelsPerTaskGroup] should all be positive numbers.");

        int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelsPerTaskGroup);

        Configuration aTaskConfig = contentConfig.get(0);

        String readerResourceMark = aTaskConfig.getString(JOB_READER_PARAMETER + "." +
                LOAD_BALANCE_RESOURCE_MARK);
        String writerResourceMark = aTaskConfig.getString(JOB_WRITER_PARAMETER + "." +
                LOAD_BALANCE_RESOURCE_MARK);

        boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) ||
                StringUtils.isNotBlank(writerResourceMark);

        if (!hasLoadBalanceResourceMark) {
            // set a fake resource mark for load balance (can be set on reader or writer, here choose to fake on reader)
            for (Configuration conf : contentConfig) {
                conf.set(JOB_READER_PARAMETER + "." +
                        LOAD_BALANCE_RESOURCE_MARK, "aFakeResourceMarkForLoadBalance");
            }
            // to avoid some plugins not setting the resource mark and performing a random shuffle
            Collections.shuffle(contentConfig, new SecureRandom());
        }

        LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap = parseAndGetResourceMarkAndTaskIdMap(contentConfig);
        List<Configuration> taskGroupConfig = doAssign(resourceMarkAndTaskIdMap, configuration, taskGroupNumber);

        // adjust channel number per task group
        adjustChannelNumPerTaskGroup(taskGroupConfig, channelNumber);
        return taskGroupConfig;
    }

    /**
     * Adjust the number of channels per task group.
     *
     * @param taskGroupConfig configuration
     * @param channelNumber the number of channel
     */
    private static void adjustChannelNumPerTaskGroup(List<Configuration> taskGroupConfig, int channelNumber)
    {
        int taskGroupNumber = taskGroupConfig.size();
        // indicates that there are remainderChannelCount taskGroups,
        // and the corresponding number of Channels should be: avgChannelsPerTaskGroup + 1;
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        int remainderChannelCount = channelNumber % taskGroupNumber;

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfig.get(i).set(CORE_CONTAINER_TASK_GROUP_CHANNEL, avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfig.get(i + j).set(CORE_CONTAINER_TASK_GROUP_CHANNEL, avgChannelsPerTaskGroup);
        }
    }


    /**
     * Parse the resource mark and taskId map.
     *
     * @param contentConfig configuration
     * @return map of resource mark and taskId
     */
    private static LinkedHashMap<String, List<Integer>> parseAndGetResourceMarkAndTaskIdMap(List<Configuration> contentConfig)
    {
        // key: resourceMark, value: taskId
        LinkedHashMap<String, List<Integer>> readerResourceMarkAndTaskIdMap = new LinkedHashMap<>();
        LinkedHashMap<String, List<Integer>> writerResourceMarkAndTaskIdMap = new LinkedHashMap<>();

        for (Configuration aTaskConfig : contentConfig) {
            int taskId = aTaskConfig.getInt(CoreConstant.TASK_ID);
            // 把 readerResourceMark 加到 readerResourceMarkAndTaskIdMap 中
            String readerResourceMark = aTaskConfig.getString(JOB_READER_PARAMETER + "." + LOAD_BALANCE_RESOURCE_MARK);
            readerResourceMarkAndTaskIdMap.computeIfAbsent(readerResourceMark, k -> new ArrayList<>());
            readerResourceMarkAndTaskIdMap.get(readerResourceMark).add(taskId);

            // 把 writerResourceMark 加到 writerResourceMarkAndTaskIdMap 中
            String writerResourceMark = aTaskConfig.getString(JOB_WRITER_PARAMETER + "." + LOAD_BALANCE_RESOURCE_MARK);
            writerResourceMarkAndTaskIdMap.computeIfAbsent(writerResourceMark, k -> new ArrayList<>());
            writerResourceMarkAndTaskIdMap.get(writerResourceMark).add(taskId);
        }

        if (readerResourceMarkAndTaskIdMap.size() >= writerResourceMarkAndTaskIdMap.size()) {
            // 采用 reader 对资源做的标记进行 shuffle
            return readerResourceMarkAndTaskIdMap;
        }
        else {
            // 采用 writer 对资源做的标记进行 shuffle
            return writerResourceMarkAndTaskIdMap;
        }
    }

    /**
     * Assign tasks to task groups.
     * The effect to be implemented is explained by example:
     * <pre>
     *     database A has tables: 0, 1, 2
     *     database B has tables: 3, 4
     *     database C has tables: 5, 6, 7
     *     If there are 4 task groups
     *     The result after assign is:
     *     taskGroup-0: 0,  4,
     *     taskGroup-1: 3,  6,
     *     taskGroup-2: 5,  2,
     *     taskGroup-3: 1,  7
     * </pre>
     *
     * @param resourceMarkAndTaskIdMap resource map
     * @param jobConfiguration configuration
     * @param taskGroupNumber the number of group
     * @return list of configuration
     */
    private static List<Configuration> doAssign(LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap, Configuration jobConfiguration, int taskGroupNumber)
    {
        List<Configuration> contentConfig = jobConfiguration.getListConfiguration(JOB_CONTENT);

        Configuration taskGroupTemplate = jobConfiguration.clone();
        taskGroupTemplate.remove(JOB_CONTENT);

        List<Configuration> result = new ArrayList<>();

        List<List<Configuration>> taskGroupConfigList = new ArrayList<>(taskGroupNumber);
        for (int i = 0; i < taskGroupNumber; i++) {
            taskGroupConfigList.add(new ArrayList<>());
        }

        int mapValueMaxLength = -1;

        List<String> resourceMarks = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        int taskGroupIndex = 0;
        for (int i = 0; i < mapValueMaxLength; i++) {
            for (String resourceMark : resourceMarks) {
                if (!resourceMarkAndTaskIdMap.get(resourceMark).isEmpty()) {
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMark).get(0);
                    taskGroupConfigList.get(taskGroupIndex % taskGroupNumber).add(contentConfig.get(taskId));
                    taskGroupIndex++;

                    resourceMarkAndTaskIdMap.get(resourceMark).remove(0);
                }
            }
        }
        Configuration tempTaskGroupConfig;
        for (int i = 0; i < taskGroupNumber; i++) {
            tempTaskGroupConfig = taskGroupTemplate.clone();
            tempTaskGroupConfig.set(JOB_CONTENT, taskGroupConfigList.get(i));
            tempTaskGroupConfig.set(CORE_CONTAINER_TASK_GROUP_ID, i);

            result.add(tempTaskGroupConfig);
        }

        return result;
    }
}
