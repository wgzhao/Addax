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
import com.wgzhao.addax.server.model.SubmitResult;
import com.wgzhao.addax.server.model.TaskInfo;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service for submitting, watching and cancelling tasks.
 *
 * <p>Every task runs in a JVM of its own, started the way the command line tool starts one. The
 * engine keeps per job settings process wide (system properties, the column formats), so running
 * jobs in a separate process is what keeps them from interfering with each other and with the
 * server; it also makes a job cancellable (destroy the process) and a crash harmless.
 */
public class TaskService
{
    private static final Logger LOG = Logger.getLogger(TaskService.class.getName());

    private static final String CONFIG_PARSER_CLASS = "com.wgzhao.addax.core.util.ConfigParser";

    private static final String ENGINE_CLASS = "com.wgzhao.addax.core.Engine";

    /**
     * Properties of the server JVM a job has to inherit to run at all. They are set on the
     * command line of the job last, so that no request parameter can override them.
     */
    private static final List<String> INHERITED_PROPERTIES = List.of(
            "addax.home", "addax.log", "logback.configurationFile", "console.enabled", "file.encoding");

    /** Response body documented for a submission that runs into the concurrency limit. */
    private static final String TOO_MANY_TASKS_MESSAGE = "ERROR: Maximum number of concurrent tasks reached.";

    /** How much of the end of a job log is read to report why the job failed. */
    private static final int ERROR_SCAN_BYTES = 64 * 1024;

    /** How long a job gets to react to a cancellation before it is killed. */
    private static final int CANCEL_GRACE_SECONDS = 10;

    private static final int MAX_ERROR_LENGTH = 500;

    private final ExecutorService executor;

    /** Seconds a job may run, 0 for no limit. */
    private final int taskTimeoutSeconds;

    private final Map<String, RunningTask> running = new ConcurrentHashMap<>();

    /**
     * Construct TaskService with an ExecutorService used to watch tasks.
     *
     * @param executor ExecutorService used to wait for the job processes
     * @param taskTimeoutSeconds seconds a job may run, 0 for no limit
     */
    public TaskService(ExecutorService executor, int taskTimeoutSeconds)
    {
        this.executor = executor;
        this.taskTimeoutSeconds = taskTimeoutSeconds;
    }

    /**
     * Submit a task for asynchronous execution.
     *
     * <p>The job is staged in a temporary file named after {@code suffix}, because that suffix is
     * what core uses to tell JSON and YAML apart. It is checked there before it is accepted, so a
     * job that cannot even be parsed is rejected right away instead of failing while nobody is
     * looking. Query parameters become {@code -D} properties of the job JVM, the way they are
     * passed to the command line tool.
     *
     * @param jobJson job JSON or YAML content from the request body
     * @param suffix file name suffix of the job, {@code .json} or {@code .yaml}
     * @param params parameters of the request, set as system properties of the job
     * @return the accepted task id or the reason the job was not accepted
     */
    public SubmitResult submitTask(String jobJson, String suffix, Map<String, String> params)
    {
        Path jobFile;
        try {
            jobFile = Files.createTempFile("addax-job-", suffix);
            Files.writeString(jobFile, jobJson == null ? "" : jobJson, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, "Cannot stage the submitted job", e);
            return new SubmitResult.Rejected(SubmitResult.Rejected.Reason.INVALID_JOB, describeFailure(e));
        }

        String problem = validate(jobFile);
        if (problem != null) {
            deleteQuietly(jobFile);
            return new SubmitResult.Rejected(SubmitResult.Rejected.Reason.INVALID_JOB, problem);
        }

        if (!TaskManager.tryAcquireSlot()) {
            deleteQuietly(jobFile);
            return new SubmitResult.Rejected(SubmitResult.Rejected.Reason.TOO_MANY_TASKS, TOO_MANY_TASKS_MESSAGE);
        }

        String taskId = UUID.randomUUID().toString();
        Process process;
        try {
            process = launch(jobFile, taskId, params);
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, "Cannot start the job process", e);
            TaskManager.releaseSlot();
            deleteQuietly(jobFile);
            throw new IllegalStateException("cannot start the job process: " + describeFailure(e), e);
        }

        TaskManager.addTask(TaskInfo.running(taskId));
        RunningTask task = new RunningTask(process, new AtomicBoolean());
        running.put(taskId, task);
        try {
            executor.submit(() -> await(taskId, jobFile, task));
        }
        catch (RuntimeException e) {
            // nothing will ever watch this process, so it and everything it holds has to go
            running.remove(taskId);
            process.destroyForcibly();
            TaskManager.updateTask(taskId, TaskInfo.Status.FAILED, null, describeFailure(e));
            TaskManager.releaseSlot();
            deleteQuietly(jobFile);
            throw e;
        }
        return new SubmitResult.Accepted(taskId);
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

    /**
     * Cancel a running task by destroying its process.
     *
     * @param taskId id of the task
     * @return whether the task was found, was still running and was cancelled
     */
    public CancelOutcome cancel(String taskId)
    {
        RunningTask task = running.get(taskId);
        if (task == null) {
            return TaskManager.getTask(taskId) == null ? CancelOutcome.NOT_FOUND : CancelOutcome.NOT_RUNNING;
        }
        try {
            cancel(task);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.process().destroyForcibly();
        }
        return CancelOutcome.CANCELLED;
    }

    /**
     * Kill the process of every running task. Registered as shutdown hook so that stopping the
     * server does not leave jobs behind that keep writing into their targets.
     */
    public void destroyRunningTasks()
    {
        running.values().forEach(task -> task.process().destroyForcibly());
    }

    /** What happened to a cancellation request. */
    public enum CancelOutcome
    {
        /** No task with that id is known. */
        NOT_FOUND,
        /** The task exists but is not running any more. */
        NOT_RUNNING,
        /** The task was running and its process has been destroyed. */
        CANCELLED
    }

    private void await(String taskId, Path jobFile, RunningTask task)
    {
        Process process = task.process();
        try {
            boolean finished;
            if (taskTimeoutSeconds <= 0) {
                process.waitFor();
                finished = true;
            }
            else {
                finished = process.waitFor(taskTimeoutSeconds, TimeUnit.SECONDS);
            }

            TaskInfo.Status status;
            String error = null;
            if (!finished) {
                cancel(task);
                status = TaskInfo.Status.CANCELLED;
                error = "cancelled after the task timeout of " + taskTimeoutSeconds + "s";
                LOG.log(Level.WARNING, "Task {0} cancelled: {1}", new Object[] {taskId, error});
            }
            else if (process.exitValue() == 0) {
                status = TaskInfo.Status.SUCCESS;
            }
            else if (task.cancelled().get()) {
                status = TaskInfo.Status.CANCELLED;
                error = "cancelled on request";
                LOG.log(Level.INFO, "Task {0} cancelled on request", taskId);
            }
            else {
                status = TaskInfo.Status.FAILED;
                error = describeExit(taskId, process.exitValue());
                LOG.log(Level.WARNING, "Task {0} failed: {1}", new Object[] {taskId, error});
            }
            TaskManager.updateTask(taskId, status, status == TaskInfo.Status.SUCCESS ? "Job executed." : null, error);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            LOG.log(Level.WARNING, "Interrupted while waiting for task " + taskId, e);
            TaskManager.updateTask(taskId, TaskInfo.Status.CANCELLED, null, "the server shut this task down");
        }
        finally {
            running.remove(taskId);
            deleteQuietly(jobFile);
            TaskManager.releaseSlot();
        }
    }

    /**
     * Ask the job to stop and make sure it does.
     *
     * @param task job to cancel
     * @throws InterruptedException when the wait for the process is interrupted
     */
    private static void cancel(RunningTask task)
            throws InterruptedException
    {
        task.cancelled().set(true);
        Process process = task.process();
        process.destroy();
        if (!process.waitFor(CANCEL_GRACE_SECONDS, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor();
        }
    }

    /**
     * Start the job in a JVM of its own, the same way the command line tool starts one.
     *
     * @param jobFile staged job configuration
     * @param taskId id of the task, used to name the log of the job
     * @param params parameters of the request, each one becomes a property of the job
     * @return the running process
     * @throws IOException when the process cannot be started
     */
    private static Process launch(Path jobFile, String taskId, Map<String, String> params)
            throws IOException
    {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        // run the job with the same JVM options the server got, so it behaves like a command
        // line run (heap, GC, agents)
        command.addAll(ManagementFactory.getRuntimeMXBean().getInputArguments());
        // request parameters act like the -D parameters of the command line tool
        params.forEach((name, value) -> command.add("-D" + name + "=" + value));
        for (String name : INHERITED_PROPERTIES) {
            String value = System.getProperty(name);
            if (value != null) {
                command.add("-D" + name + "=" + value);
            }
        }
        command.add("-Dlog.file.name=" + logFileName(taskId));
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ENGINE_CLASS);
        command.add("-job");
        command.add(jobFile.toString());

        // the job writes its console output where the server writes it, the log file of the job
        // is the place to look for details of a failure. Its input comes from /dev/null so that a
        // job cannot take over the terminal of the server.
        return new ProcessBuilder(command)
                .redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")))
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
    }

    /**
     * Check the staged job with the parser the job is going to use, so that an unacceptable job
     * is rejected at submission time rather than failing while nobody is looking.
     *
     * <p>Core is on the class path at runtime only, so the parser is looked up by reflection.
     * When it cannot be found the job is accepted unvalidated and the task reports the problem.
     *
     * @param jobFile staged job configuration
     * @return why the job is not acceptable, or null when it is
     */
    private static String validate(Path jobFile)
    {
        Method parse;
        try {
            parse = Class.forName(CONFIG_PARSER_CLASS).getMethod("parse", String.class);
        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            return null;
        }
        try {
            parse.invoke(null, jobFile.toString());
            return null;
        }
        catch (InvocationTargetException e) {
            return describeFailure(e.getTargetException());
        }
        catch (ReflectiveOperationException e) {
            return null;
        }
    }

    /**
     * Describe why a job process failed: the last error it logged, plus the exit code.
     *
     * @param taskId id of the task, its log file is looked up with it
     * @param exitCode exit code of the job process
     * @return error message for the status of the task
     */
    private static String describeExit(String taskId, int exitCode)
    {
        String logged = lastErrorLine(taskId);
        if (logged == null) {
            return "the job process exited with code " + exitCode;
        }
        return logged + " (exit code " + exitCode + ", see " + logFileName(taskId) + ")";
    }

    /**
     * Read the last line a job logged as an error, or its last line of output when it logged no
     * error at all. The job logs into {@code $addax.log} with logback, so only the tail of the
     * file is read.
     *
     * @param taskId id of the task
     * @return the line to report, or null when the log cannot be read
     */
    private static String lastErrorLine(String taskId)
    {
        String logDir = System.getProperty("addax.log");
        if (logDir == null || logDir.isBlank()) {
            return null;
        }
        Path log = Path.of(logDir, logFileName(taskId));
        String[] lines;
        try {
            long size = Files.size(log);
            long from = Math.max(0, size - ERROR_SCAN_BYTES);
            ByteBuffer buffer = ByteBuffer.allocate((int) (size - from));
            try (SeekableByteChannel channel = Files.newByteChannel(log)) {
                channel.position(from);
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                    // read the tail of the log
                }
            }
            lines = new String(buffer.array(), 0, buffer.position(), StandardCharsets.UTF_8).split("\n");
        }
        catch (IOException e) {
            return null;
        }

        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (line.contains(" ERROR ") || line.startsWith("ERROR ")) {
                return abbreviate(line);
            }
        }
        for (int i = lines.length - 1; i >= 0; i--) {
            if (!lines[i].isBlank()) {
                return abbreviate(lines[i].trim());
            }
        }
        return null;
    }

    private static String abbreviate(String line)
    {
        return line.length() <= MAX_ERROR_LENGTH ? line : line.substring(0, MAX_ERROR_LENGTH) + "...";
    }

    private static String logFileName(String taskId)
    {
        return "addax-" + taskId + ".log";
    }

    /**
     * Describe a failure with the innermost cause, which is what callers need to act on.
     *
     * @param e failure thrown while handling the task
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

    private static void deleteQuietly(Path file)
    {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        }
        catch (IOException ignored) {
            // the file lives in the temp directory, a leftover is not worth failing a task for
        }
    }

    /** A job process together with the flag that tells a cancellation from a failure. */
    private record RunningTask(Process process, AtomicBoolean cancelled) {}
}
