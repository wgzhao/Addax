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
 * Outcome of a job submission.
 */
public sealed interface SubmitResult
{
    /**
     * The job was accepted and is executed asynchronously.
     *
     * @param taskId id to poll the task with
     */
    record Accepted(String taskId) implements SubmitResult {}

    /**
     * The job was not accepted.
     */
    record Rejected(Reason reason, String message) implements SubmitResult
    {
        /** Why a submission was rejected; the HTTP layer maps it to a status code. */
        public enum Reason
        {
            /** The maximum number of concurrent tasks is reached. */
            TOO_MANY_TASKS,
            /** The job configuration is not acceptable, e.g. it cannot be parsed. */
            INVALID_JOB
        }
    }
}
