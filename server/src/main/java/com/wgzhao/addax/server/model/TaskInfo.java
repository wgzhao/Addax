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

package com.wgzhao.addax.server.model;

/**
 * Immutable snapshot of a task.
 *
 * <p>A snapshot is replaced as a whole in the task map, so a reader polling the status either
 * sees the previous state or the new one, never a mix of both.
 *
 * @param taskId unique id of the task
 * @param status current status
 * @param result result message, {@code null} until the task has succeeded
 * @param error error message, {@code null} unless the task has failed
 */
public record TaskInfo(String taskId, Status status, String result, String error)
{
    /** Task status. */
    public enum Status
    {
        RUNNING, SUCCESS, FAILED
    }

    /**
     * Create the initial snapshot of a running task.
     *
     * @param taskId unique id of the task
     * @return a RUNNING snapshot without result and error
     */
    public static TaskInfo running(String taskId)
    {
        return new TaskInfo(taskId, Status.RUNNING, null, null);
    }
}
