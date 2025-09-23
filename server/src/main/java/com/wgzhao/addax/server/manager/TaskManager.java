/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.server.manager;

import com.wgzhao.addax.server.model.TaskInfo;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.Map;

/**
 * Manages the lifecycle, status, and results of tasks.
 * Controls the maximum number of concurrent running tasks.
 */
public class TaskManager {
    private static final Map<String, TaskInfo> tasks = new ConcurrentHashMap<>();
    private static Semaphore semaphore = new Semaphore(30); // Default max concurrent tasks

    /**
     * Set the maximum number of concurrent running tasks.
     * @param maxTasks maximum concurrent tasks
     */
    public static void setMaxConcurrentTasks(int maxTasks) {
        semaphore = new Semaphore(maxTasks);
    }

    /**
     * Try to acquire a slot for a new running task.
     * @return true if acquired, false if limit reached
     */
    public static boolean tryAcquireSlot() {
        return semaphore.tryAcquire();
    }

    /**
     * Release a slot when a task finishes.
     */
    public static void releaseSlot() {
        semaphore.release();
    }

    /**
     * Add a new task to the manager.
     * @param taskInfo task information
     */
    public static void addTask(TaskInfo taskInfo) {
        tasks.put(taskInfo.getTaskId(), taskInfo);
    }

    /**
     * Get task information by task ID.
     * @param taskId task ID
     * @return TaskInfo or null if not found
     */
    public static TaskInfo getTask(String taskId) {
        return tasks.get(taskId);
    }

    /**
     * Update the status and result of a task.
     * @param taskId task ID
     * @param status task status
     * @param result result string
     * @param error error message
     */
    public static void updateTask(String taskId, TaskInfo.Status status, String result, String error) {
        TaskInfo info = tasks.get(taskId);
        if (info != null) {
            info.setStatus(status);
            info.setResult(result);
            info.setError(error);
        }
    }
}
