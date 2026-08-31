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

/** Task Info. */
public class TaskInfo {
    /** Status. */
    public enum Status {
        RUNNING, SUCCESS, FAILED
    }

    private final String taskId;
    private Status status;
    private String result;
    private String error;

    /** Taskinfo. */
    public TaskInfo(String taskId) {
        this.taskId = taskId;
        this.status = Status.RUNNING;
    }

    // getters and setters
    /** Returns the taskid. */
    public String getTaskId() { return taskId; }
    /** Returns the status. */
    public Status getStatus() { return status; }
    /** Sets the status. */
    public void setStatus(Status status) { this.status = status; }
    /** Returns the result. */
    public String getResult() { return result; }
    /** Sets the result. */
    public void setResult(String result) { this.result = result; }
    /** Returns the error. */
    public String getError() { return error; }
    /** Sets the error. */
    public void setError(String error) { this.error = error; }
}
