/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.server.service;

import com.wgzhao.addax.server.manager.TaskManager;
import com.wgzhao.addax.server.model.TaskInfo;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Service for submitting and executing tasks using a provided ExecutorService.
 * This implementation uses only JDK classes (no external web framework) and
 * invokes Addax Engine via reflection.
 */
public class TaskService {
    private final ExecutorService executor;

    /**
     * Construct TaskService with an ExecutorService used to run tasks asynchronously.
     * @param executor ExecutorService instance
     */
    public TaskService(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Submit a task with explicit parameters. The job JSON must be provided in the body
     * of the POST request. Other parameters (e.g. name, jvm) are provided as query params.
     * This method accepts a raw jvmParam string (can be JSON object) and will try to parse it.
     * @param name job name, may be null
     * @param jobJson job JSON content from request body
     * @param jvmParam optional jvm parameter string (JSON object or empty)
     * @return taskId if accepted, or an error string starting with "ERROR:"
     */
    public String submitTask(String jobJson, Map<String, String> params) {

        // currently name and jvmMap are passed to task execution; jvmMap is not used to spawn JVMs
        if (!TaskManager.tryAcquireSlot()) {
            return "ERROR: Maximum number of concurrent tasks reached.";
        }
        String taskId = UUID.randomUUID().toString();
        TaskInfo info = new TaskInfo(taskId);
        TaskManager.addTask(info);

        executor.submit(() -> runTask(taskId, "addax", jobJson));
        return taskId;
    }

    /**
     * Backwards compatible method that expects the old wrapper JSON. Maintained for internal use only.
     * @param body old wrapper body
     * @return taskId or error
     */
//    public String submitTaskFromJson(String body) {
//        // try to extract top-level job and name if client used old format
//        Map<String, String> top = parseTopLevelJson(body);
//        String name = top.get("name");
//        String job = top.get("job");
//        String jvm = top.get("jvm");
//        return submitTask(name, job, jvm);
//    }

    /**
     * Get task information.
     * @param taskId task id
     * @return TaskInfo or null
     */
    public TaskInfo getTaskInfo(String taskId) {
        return TaskManager.getTask(taskId);
    }

    private void runTask(String taskId, String name, String jobJson) {
        java.nio.file.Path tmp = null;
        try {
            // write job JSON to a temporary file
            tmp = java.nio.file.Files.createTempFile("addax-job-", ".json");
            java.nio.file.Files.writeString(tmp, jobJson == null ? "" : jobJson, java.nio.charset.StandardCharsets.UTF_8);

            // Call Engine.entry(String[] args) to run job without invoking System.exit
            Class<?> engineClass = Class.forName("com.wgzhao.addax.core.Engine");
            try {
                Method entryMethod = engineClass.getMethod("entry", String[].class);
                String[] args = new String[]{"-job", tmp.toString()};
                // invoke static method; cast to Object to avoid varargs expansion
                entryMethod.invoke(null, (Object) args);
            } catch (NoSuchMethodException nsme) {
                // Fallback: try calling main(String[]), but main calls System.exit so avoid it.
                throw new RuntimeException("Engine.entry(String[]) not found; cannot safely invoke Engine.main from server process.", nsme);
            }

            TaskManager.updateTask(taskId, TaskInfo.Status.SUCCESS, "Job " + name + " executed.", null);
        } catch (Throwable e) {
            TaskManager.updateTask(taskId, TaskInfo.Status.FAILED, null, e.toString());
        } finally {
            // cleanup temp file
            if (tmp != null) {
                try { java.nio.file.Files.deleteIfExists(tmp); } catch (Exception ignored) {}
            }
            TaskManager.releaseSlot();
        }
    }
}
