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

package com.wgzhao.addax.core.meta;

/**
 * The status of a task, or of an aggregate of tasks. The rules that move a task
 * between these statuses, and the priority with which two of them merge, are the
 * state machine in {@link TaskState}.
 */
public enum State
{
    RUNNING(30),
    FAILED(60),
    SUCCEEDED(70),
    ;

    private final int value;

    State(int value)
    {
        this.value = value;
    }

    public int value()
    {
        return this.value;
    }
}
