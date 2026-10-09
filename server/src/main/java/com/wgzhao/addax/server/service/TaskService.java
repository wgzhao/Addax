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

package com.wgzhao.addax.server.service;

import com.wgzhao.addax.server.manager.TaskManager;
import com.wgzhao.addax.server.model.TaskInfo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for submitting and executing tasks using a provided ExecutorService.
 * This implementation uses only JDK classes (no external web framework) and
 * invokes Addax Engine via reflection.
 */
public class TaskService
{
    private static final Logger LOG = Logger.getLogger(TaskService.class.getName());

    private static final String ENGINE_CLASS = "com.wgzhao.addax.core.Engine";

    /**
     * The same placeholder syntax core resolves from system properties, see
     * {@code com.wgzhao.addax.core.util.StrUtil#VARIABLE_PATTERN}. It is kept identical on
     * purpose: anything not expanded here is left for core to resolve.
     */
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("(\\$)\\{?(\\w+)\\}?");

    private final ExecutorService executor;

    /**
     * Construct TaskService with an ExecutorService used to run tasks asynchronously.
     *
     * @param executor ExecutorService instance
     */
    public TaskService(ExecutorService executor)
    {
        this.executor = executor;
    }

    /**
     * Submit a task for asynchronous execution. The job JSON must be provided in the body
     * of the POST request, the query parameters are expanded as job variables.
     *
     * @param jobJson job JSON content from request body
     * @param params parameters of the request, expanded into the placeholders of the job JSON
     * @return taskId if accepted, or an error string starting with "ERROR:"
     */
    public String submitTask(String jobJson, Map<String, String> params)
    {
        if (!TaskManager.tryAcquireSlot()) {
            return "ERROR: Maximum number of concurrent tasks reached.";
        }
        String taskId = UUID.randomUUID().toString();
        TaskManager.addTask(TaskInfo.running(taskId));

        executor.submit(() -> runTask(taskId, params, jobJson));
        return taskId;
    }

    /**
     * Get task information.
     *
     * @param taskId task id
     * @return TaskInfo or null
     */
    public TaskInfo getTaskInfo(String taskId)
    {
        return TaskManager.getTask(taskId);
    }

    private void runTask(String taskId, Map<String, String> params, String jobJson)
    {
        Path tmp = null;
        try {
            tmp = Files.createTempFile("addax-job-", ".json");
            Files.writeString(tmp, expandVariables(jobJson == null ? "" : jobJson, params), StandardCharsets.UTF_8);

            invokeEngine(tmp.toString());
            TaskManager.updateTask(taskId, TaskInfo.Status.SUCCESS, "Job executed.", null);
        }
        catch (Throwable e) {
            LOG.log(Level.SEVERE, "Task " + taskId + " failed", e);
            TaskManager.updateTask(taskId, TaskInfo.Status.FAILED, null, describeFailure(e));
        }
        finally {
            // cleanup temp file
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                }
                catch (Exception ignored) {
                }
            }
            TaskManager.releaseSlot();
        }
    }

    private static void invokeEngine(String jobPath)
            throws Throwable
    {
        // Call Engine.entry(String[] args) to run job without invoking System.exit
        Method entryMethod;
        try {
            entryMethod = Class.forName(ENGINE_CLASS).getMethod("entry", String[].class);
        }
        catch (NoSuchMethodException nsme) {
            // Fallback: try calling main(String[]), but main calls System.exit so avoid it.
            throw new IllegalStateException("Engine.entry(String[]) not found; cannot safely invoke Engine.main from server process.", nsme);
        }

        try {
            // invoke static method; cast to Object to avoid varargs expansion
            entryMethod.invoke(null, (Object) new String[] {"-job", jobPath});
        }
        catch (InvocationTargetException e) {
            // rethrow what the engine actually failed with; the cause is the only useful part
            throw e.getTargetException();
        }
    }

    /**
     * Expand {@code ${name}} / {@code $name} placeholders with the parameters of this request.
     *
     * <p>Core resolves these placeholders from the process wide system properties. Writing the
     * request parameters there instead (as this service used to do) leaks them into every
     * concurrent and every later task, so the expansion is done on the job text of this task only.
     * Placeholders without a matching parameter are left untouched for core to resolve.
     *
     * @param jobText raw job configuration
     * @param params parameters of the request
     * @return job configuration with this request's parameters expanded
     */
    static String expandVariables(String jobText, Map<String, String> params)
    {
        if (jobText.isEmpty() || params.isEmpty()) {
            return jobText;
        }
        Matcher matcher = VARIABLE_PATTERN.matcher(jobText);
        StringBuilder expanded = new StringBuilder(jobText.length());
        while (matcher.find()) {
            String value = params.get(matcher.group(2));
            matcher.appendReplacement(expanded, Matcher.quoteReplacement(value == null ? matcher.group() : value));
        }
        matcher.appendTail(expanded);
        return expanded.toString();
    }

    /**
     * Describe a failure with the innermost cause, which is what callers need to act on.
     *
     * @param e failure thrown while running the task
     * @return the failure and, when it differs, its root cause
     */
    static String describeFailure(Throwable e)
    {
        String description = e.toString();
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root != e && !Objects.equals(root.toString(), description)) {
            description = description + " (caused by: " + root + ")";
        }
        return description;
    }
}
