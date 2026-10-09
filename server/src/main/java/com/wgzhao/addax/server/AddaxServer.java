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

package com.wgzhao.addax.server;

import com.wgzhao.addax.server.manager.TaskManager;
import com.wgzhao.addax.server.model.SubmitResult;
import com.wgzhao.addax.server.model.TaskInfo;
import com.wgzhao.addax.server.service.TaskService;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal HTTP server using JDK HttpServer. Provides /api/submit, /api/status and /api/cancel
 * endpoints.
 *
 * <p>Every job runs in a JVM of its own, so the per job settings of the engine cannot leak
 * between jobs and a job can be cancelled or crash without taking the server with it.
 */
public class AddaxServer
{
    private static final int DEFAULT_PORT = 10601;
    private static final int DEFAULT_PARALLEL = 30;
    private static final int DEFAULT_MAX_TASKS = 10000;
    private static final int DEFAULT_TASK_TIMEOUT = 0;

    /** Reject request bodies above this size; a job configuration has no business being bigger. */
    private static final int MAX_BODY_BYTES = 8 * 1024 * 1024;

    private static final String JSON_SUFFIX = ".json";
    private static final String YAML_SUFFIX = ".yaml";

    /**
     * Main entry.
     *
     * @param args command line arguments, see {@link #usage(PrintStream)}
     * @throws Exception on startup error
     */
    public static void main(String[] args)
            throws Exception
    {
        for (String arg : args) {
            if ("-h".equals(arg) || "--help".equals(arg)) {
                usage(System.out);
                return;
            }
        }

        Settings settings;
        try {
            settings = Settings.parse(args);
        }
        catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            usage(System.err);
            System.exit(2);
            return;
        }

        TaskManager.setMaxConcurrentTasks(settings.parallel());
        TaskManager.setMaxRetainedTasks(settings.maxTasks());
        // the number of running jobs is bounded by the limit of the TaskManager, so a fixed pool
        // of the same size is enough and keeps the threads named
        ExecutorService taskExecutor = Executors.newFixedThreadPool(settings.parallel(), namedThreads("addax-task-"));
        TaskService taskService = new TaskService(taskExecutor, settings.taskTimeout());
        Runtime.getRuntime().addShutdownHook(new Thread(taskService::destroyRunningTasks, "addax-shutdown"));

        InetSocketAddress address = settings.bind() == null
                ? new InetSocketAddress(settings.port())
                : new InetSocketAddress(settings.bind(), settings.port());
        HttpServer server = HttpServer.create(address, 0);
        server.createContext("/api/submit", new SubmitHandler(taskService));
        server.createContext("/api/status", new StatusHandler(taskService));
        server.createContext("/api/cancel", new CancelHandler(taskService));
        server.setExecutor(Executors.newFixedThreadPool(Math.max(2, settings.parallel()), namedThreads("addax-http-")));

        System.out.println("Starting Addax minimal HTTP server on " + address + " with maxParallel=" + settings.parallel());
        server.start();
    }

    private static void usage(PrintStream out)
    {
        out.println("""
                Usage: AddaxServer [options]

                  -p, --parallel <n>   maximum number of concurrently running tasks,
                                       default 30 or $ADDAX_SERVER_PARALLEL
                  --port <port>        port to listen on, default 10601
                  --bind <address>     address to bind to, default all interfaces
                  --max-tasks <n>      finished tasks kept for status queries, default 10000
                  --task-timeout <n>   seconds a job may run before it is cancelled, default no limit
                  -h, --help           show this help""");
    }

    private static ThreadFactory namedThreads(String prefix)
    {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> new Thread(runnable, prefix + sequence.incrementAndGet());
    }

    private static int environmentInt(String name, int fallback)
    {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            int number = Integer.parseInt(value.trim());
            if (number < 1) {
                throw new NumberFormatException();
            }
            return number;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a positive integer, got '" + value + "'");
        }
    }

    /**
     * Read the request body.
     *
     * @param exchange exchange to read the body of
     * @return body decoded as UTF-8
     * @throws BodyTooLargeException when the body is larger than {@link #MAX_BODY_BYTES}
     * @throws IOException when the body cannot be read
     */
    static String readRequestBody(HttpExchange exchange)
            throws IOException
    {
        byte[] data = exchange.getRequestBody().readNBytes(MAX_BODY_BYTES + 1);
        if (data.length > MAX_BODY_BYTES) {
            throw new BodyTooLargeException("request body is larger than " + MAX_BODY_BYTES + " bytes");
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    static void writeJsonResponse(HttpExchange exchange, int statusCode, String json)
            throws IOException
    {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    /**
     * Reject a method the endpoint does not offer. RFC 7231 requires the answer to name the
     * methods that are allowed.
     *
     * @param exchange exchange to answer
     * @param allowed methods the endpoint allows
     * @throws IOException when the response cannot be written
     */
    static void methodNotAllowed(HttpExchange exchange, String allowed)
            throws IOException
    {
        exchange.getResponseHeaders().add("Allow", allowed);
        exchange.sendResponseHeaders(405, -1);
    }

    static Map<String, String> parseQueryParams(String query)
    {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return params;
        }
        String[] pairs = query.split("&");
        for (String p : pairs) {
            int idx = p.indexOf('=');
            if (idx > 0) {
                String k = URLDecoder.decode(p.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(p.substring(idx + 1), StandardCharsets.UTF_8);
                params.put(k, v);
            }
            else if (!p.isEmpty()) {
                String k = URLDecoder.decode(p, StandardCharsets.UTF_8);
                params.put(k, "");
            }
        }
        return params;
    }

    /**
     * Pick the file name suffix of the staged job: core tells JSON and YAML apart by suffix, so
     * the format has to be decided here. The content type of the request is authoritative, and a
     * body that does not start like JSON is taken as YAML.
     *
     * @param contentType value of the Content-Type header, may be null
     * @param body request body
     * @return {@code .json} or {@code .yaml}
     */
    static String jobSuffix(String contentType, String body)
    {
        if (contentType != null) {
            String type = contentType.toLowerCase(Locale.ROOT);
            if (type.contains("yaml") || type.contains("yml")) {
                return YAML_SUFFIX;
            }
            if (type.contains("json")) {
                return JSON_SUFFIX;
            }
        }
        String trimmed = body.stripLeading();
        return !trimmed.isEmpty() && (trimmed.charAt(0) == '{' || trimmed.charAt(0) == '[') ? JSON_SUFFIX : YAML_SUFFIX;
    }

    record SubmitHandler(TaskService taskService)
            implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange)
                throws IOException
        {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                methodNotAllowed(exchange, "POST");
                return;
            }
            String body;
            try {
                body = readRequestBody(exchange);
            }
            catch (BodyTooLargeException e) {
                writeJsonResponse(exchange, 413, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
                return;
            }
            if (body.isEmpty()) {
                writeJsonResponse(exchange, 400, "{\"error\":\"missing job JSON in request body\"}");
                return;
            }
            String suffix = jobSuffix(exchange.getRequestHeaders().getFirst("Content-Type"), body);
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
            try {
                SubmitResult result = taskService.submitTask(body, suffix, params);
                if (result instanceof SubmitResult.Accepted accepted) {
                    writeJsonResponse(exchange, 200, "{\"taskId\":\"" + escapeJson(accepted.taskId()) + "\"}");
                }
                else if (result instanceof SubmitResult.Rejected rejected) {
                    boolean tooManyTasks = rejected.reason() == SubmitResult.Rejected.Reason.TOO_MANY_TASKS;
                    writeJsonResponse(exchange, tooManyTasks ? 429 : 400,
                            "{\"error\":\"" + escapeJson(rejected.message()) + "\"}");
                }
            }
            catch (Exception e) {
                writeJsonResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    record StatusHandler(TaskService taskService)
            implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange)
                throws IOException
        {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                methodNotAllowed(exchange, "GET");
                return;
            }
            String taskId = parseQueryParams(exchange.getRequestURI().getQuery()).get("taskId");
            if (taskId == null || taskId.isEmpty()) {
                writeJsonResponse(exchange, 400, "{\"error\":\"missing taskId\"}");
                return;
            }
            TaskInfo info = taskService.getTaskInfo(taskId);
            if (info == null) {
                writeJsonResponse(exchange, 404, "{\"error\":\"task not found\"}");
                return;
            }
            String json = "{\"taskId\":\"" + escapeJson(info.taskId()) + "\"," +
                    "\"status\":\"" + info.status().name() + "\"," +
                    "\"result\":\"" + escapeJson(info.result()) + "\"," +
                    "\"error\":\"" + escapeJson(info.error()) + "\"}";
            writeJsonResponse(exchange, 200, json);
        }
    }

    record CancelHandler(TaskService taskService)
            implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange)
                throws IOException
        {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                methodNotAllowed(exchange, "POST");
                return;
            }
            String taskId = parseQueryParams(exchange.getRequestURI().getQuery()).get("taskId");
            if (taskId == null || taskId.isEmpty()) {
                writeJsonResponse(exchange, 400, "{\"error\":\"missing taskId\"}");
                return;
            }
            switch (taskService.cancel(taskId)) {
                case NOT_FOUND -> writeJsonResponse(exchange, 404, "{\"error\":\"task not found\"}");
                case NOT_RUNNING -> writeJsonResponse(exchange, 409, "{\"error\":\"task is not running\"}");
                case CANCELLED -> writeJsonResponse(exchange, 200,
                        "{\"taskId\":\"" + escapeJson(taskId) + "\",\"status\":\"CANCELLED\"}");
            }
        }
    }

    /**
     * Escape a string for use inside a JSON string literal.
     *
     * <p>Every character below U+0020 must be escaped, otherwise the response is not valid JSON
     * for a strict parser (error messages may well contain tabs or other control characters).
     *
     * @param s string to escape, may be null
     * @return escaped string, or an empty string for null
     */
    static String escapeJson(String s)
    {
        if (s == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    }
                    else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
    }

    /** Startup settings. */
    record Settings(int port, int parallel, int maxTasks, int taskTimeout, InetAddress bind)
    {
        /**
         * Parse the command line, falling back to the environment and to the defaults.
         *
         * @param args command line arguments
         * @return the settings to start with
         * @throws IllegalArgumentException when an option is unknown, incomplete or out of range
         */
        static Settings parse(String[] args)
        {
            int port = DEFAULT_PORT;
            int parallel = environmentInt("ADDAX_SERVER_PARALLEL", DEFAULT_PARALLEL);
            int maxTasks = DEFAULT_MAX_TASKS;
            int taskTimeout = DEFAULT_TASK_TIMEOUT;
            InetAddress bind = null;

            for (int i = 0; i < args.length; i++) {
                String option = args[i];
                switch (option) {
                    case "-p", "--parallel" -> parallel = positive(valueOf(args, ++i, option), option);
                    case "--port" -> port = inRange(valueOf(args, ++i, option), option, 1, 65535);
                    case "--max-tasks" -> maxTasks = positive(valueOf(args, ++i, option), option);
                    case "--task-timeout" -> taskTimeout = notNegative(valueOf(args, ++i, option), option);
                    case "--bind" -> bind = address(valueOf(args, ++i, option), option);
                    default -> throw new IllegalArgumentException("unknown option: " + option);
                }
            }
            return new Settings(port, parallel, maxTasks, taskTimeout, bind);
        }

        private static String valueOf(String[] args, int index, String option)
        {
            if (index >= args.length) {
                throw new IllegalArgumentException("missing value for " + option);
            }
            return args[index];
        }

        private static int number(String value, String option)
        {
            try {
                return Integer.parseInt(value);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(option + " expects a number, got '" + value + "'");
            }
        }

        private static int positive(String value, String option)
        {
            int number = number(value, option);
            if (number < 1) {
                throw new IllegalArgumentException(option + " must be at least 1, got " + number);
            }
            return number;
        }

        private static int notNegative(String value, String option)
        {
            int number = number(value, option);
            if (number < 0) {
                throw new IllegalArgumentException(option + " must not be negative, got " + number);
            }
            return number;
        }

        private static int inRange(String value, String option, int min, int max)
        {
            int number = number(value, option);
            if (number < min || number > max) {
                throw new IllegalArgumentException(option + " must be between " + min + " and " + max + ", got " + number);
            }
            return number;
        }

        private static InetAddress address(String value, String option)
        {
            try {
                return InetAddress.getByName(value);
            }
            catch (UnknownHostException e) {
                throw new IllegalArgumentException(option + " cannot be resolved: " + value);
            }
        }
    }

    /** Raised when a request body exceeds {@link #MAX_BODY_BYTES}. */
    static class BodyTooLargeException
            extends IOException
    {
        private static final long serialVersionUID = 1L;

        BodyTooLargeException(String message)
        {
            super(message);
        }
    }
}
