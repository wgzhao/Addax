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

import com.wgzhao.addax.core.config.JobConfig;
import com.wgzhao.addax.core.config.TaskConfig;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.StringUtils;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JobAssignUtil
{
    private JobAssignUtil()
    {
    }

    /**
     * Assign tasks to task groups fairly.
     * Fairness is reflected in the more balanced job distribution based on the load identification of the resource load in the task.
     *
     * @param jobConfig job configuration
     * @param channelNumber the number of channel
     * @param channelsPerTaskGroup the channel of task group
     * @return list of job configuration
     */
    public static List<JobConfig> assignFairly(JobConfig jobConfig, int channelNumber, int channelsPerTaskGroup)
    {
        Validate.isTrue(jobConfig != null, "The `job.content` can not be null.");

        List<TaskConfig> contentConfig = jobConfig.getTaskConfigs();
        Validate.isTrue(!contentConfig.isEmpty(), "The `job.content` is empty");

        Validate.isTrue(channelNumber > 0 && channelsPerTaskGroup > 0,
                "The average number of tasks per channel [averTaskPerChannel], the number of channels [channelNumber], and the average number of channels per task group " +
                        "[channelsPerTaskGroup] should all be positive numbers.");

        int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelsPerTaskGroup);

        TaskConfig aTaskConfig = contentConfig.get(0);

        String readerResourceMark = aTaskConfig.getReaderResourceMark();
        String writerResourceMark = aTaskConfig.getWriterResourceMark();

        boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) ||
                StringUtils.isNotBlank(writerResourceMark);

        if (!hasLoadBalanceResourceMark) {
            // set a fake resource mark for load balance (can be set on reader or writer, here choose to fake on reader)
            for (TaskConfig conf : contentConfig) {
                conf.setReaderResourceMark("aFakeResourceMarkForLoadBalance");
            }
            // to avoid some plugins not setting the resource mark and performing a random shuffle
            Collections.shuffle(contentConfig, new SecureRandom());
        }

        LinkedHashMap<String, ArrayDeque<Integer>> resourceMarkAndTaskIdMap = parseAndGetResourceMarkAndTaskIdMap(contentConfig);
        List<JobConfig> taskGroupConfig = doAssign(resourceMarkAndTaskIdMap, jobConfig, taskGroupNumber);

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
    private static void adjustChannelNumPerTaskGroup(List<JobConfig> taskGroupConfig, int channelNumber)
    {
        int taskGroupNumber = taskGroupConfig.size();
        // indicates that there are remainderChannelCount taskGroups,
        // and the corresponding number of Channels should be: avgChannelsPerTaskGroup + 1;
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        int remainderChannelCount = channelNumber % taskGroupNumber;

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfig.get(i).setTaskGroupChannel(avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfig.get(i + j).setTaskGroupChannel(avgChannelsPerTaskGroup);
        }
    }


    /**
     * Parse the resource mark and taskId map.
     *
     * @param contentConfig configuration
     * @return map of resource mark and taskId
     */
    private static LinkedHashMap<String, ArrayDeque<Integer>> parseAndGetResourceMarkAndTaskIdMap(List<TaskConfig> contentConfig)
    {
        // key: resourceMark, value: taskId
        LinkedHashMap<String, ArrayDeque<Integer>> readerResourceMarkAndTaskIdMap = new LinkedHashMap<>();
        LinkedHashMap<String, ArrayDeque<Integer>> writerResourceMarkAndTaskIdMap = new LinkedHashMap<>();

        for (TaskConfig aTaskConfig : contentConfig) {
            int taskId = aTaskConfig.getTaskId();
            // Add readerResourceMark to readerResourceMarkAndTaskIdMap
            String readerResourceMark = aTaskConfig.getReaderResourceMark();
            readerResourceMarkAndTaskIdMap.computeIfAbsent(readerResourceMark, k -> new ArrayDeque<>());
            readerResourceMarkAndTaskIdMap.get(readerResourceMark).add(taskId);

            // Add writerResourceMark to writerResourceMarkAndTaskIdMap
            String writerResourceMark = aTaskConfig.getWriterResourceMark();
            writerResourceMarkAndTaskIdMap.computeIfAbsent(writerResourceMark, k -> new ArrayDeque<>());
            writerResourceMarkAndTaskIdMap.get(writerResourceMark).add(taskId);
        }

        if (readerResourceMarkAndTaskIdMap.size() >= writerResourceMarkAndTaskIdMap.size()) {
            // Use reader's resource mark for shuffling
            return readerResourceMarkAndTaskIdMap;
        }
        else {
            // Use writer's resource mark for shuffling
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
    private static List<JobConfig> doAssign(LinkedHashMap<String, ArrayDeque<Integer>> resourceMarkAndTaskIdMap, JobConfig jobConfiguration, int taskGroupNumber)
    {
        List<TaskConfig> contentConfig = jobConfiguration.getTaskConfigs();

        JobConfig taskGroupTemplate = jobConfiguration.copy();
        taskGroupTemplate.removeTaskConfigs();

        List<JobConfig> result = new ArrayList<>();

        List<List<TaskConfig>> taskGroupConfigList = new ArrayList<>(taskGroupNumber);
        for (int i = 0; i < taskGroupNumber; i++) {
            taskGroupConfigList.add(new ArrayList<>());
        }

        int mapValueMaxLength = -1;

        List<String> resourceMarks = new ArrayList<>();
        for (Map.Entry<String, ArrayDeque<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        int taskGroupIndex = 0;
        for (int i = 0; i < mapValueMaxLength; i++) {
            for (String resourceMark : resourceMarks) {
                if (!resourceMarkAndTaskIdMap.get(resourceMark).isEmpty()) {
                    // pollFirst is O(1) while ArrayList.remove(0) is O(n), quadratic for many splits
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMark).pollFirst();
                    taskGroupConfigList.get(taskGroupIndex % taskGroupNumber).add(contentConfig.get(taskId));
                    taskGroupIndex++;
                }
            }
        }
        JobConfig tempTaskGroupConfig;
        for (int i = 0; i < taskGroupNumber; i++) {
            tempTaskGroupConfig = taskGroupTemplate.copy();
            tempTaskGroupConfig.setTaskConfigs(taskGroupConfigList.get(i));
            tempTaskGroupConfig.setTaskGroupId(i);

            result.add(tempTaskGroupConfig);
        }

        return result;
    }
}
