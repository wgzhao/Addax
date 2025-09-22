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
import com.wgzhao.addax.server.service.TaskService;
import com.wgzhao.addax.server.model.TaskInfo;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map;
import java.util.HashMap;
import java.net.URLDecoder;

/**
 * Minimal HTTP server using JDK HttpServer. Provides /api/submit and /api/status endpoints.
 */
public class ServerApplication {
    private static final int DEFAULT_PORT = 10601;
    private static final int DEFAULT_PARALLEL = 30;

    /**
     * Main entry. Accepts optional args: {@code -p|--parallel &lt;n&gt;} and {@code --port &lt;port&gt;}.
     * @param args command line arguments
     * @throws Exception on startup error
     */
    public static void main(String[] args) throws Exception {
        int port = DEFAULT_PORT;
        int parallel = DEFAULT_PARALLEL;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                case "--parallel":
                    if (i + 1 < args.length) {
                        try { parallel = Integer.parseInt(args[++i]); } catch (NumberFormatException ignored) {}
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        try { port = Integer.parseInt(args[++i]); } catch (NumberFormatException ignored) {}
                    }
                    break;
                default:
                    // ignore
            }
        }

        TaskManager.setMaxConcurrentTasks(parallel);
        ExecutorService executor = Executors.newCachedThreadPool();
        TaskService taskService = new TaskService(executor);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/api/submit", new SubmitHandler(taskService));
        server.createContext("/api/status", new StatusHandler(taskService));
        server.setExecutor(Executors.newFixedThreadPool(Math.max(2, parallel)));

        System.out.println("Starting Addax minimal HTTP server on port " + port + " with maxParallel=" + parallel);
        server.start();
    }

    static String readRequestBody(HttpExchange exchange) throws IOException {
        InputStream in = exchange.getRequestBody();
        byte[] data = in.readAllBytes();
        return new String(data, StandardCharsets.UTF_8);
    }

    static void writeJsonResponse(HttpExchange exchange, int statusCode, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    static Map<String, String> parseQueryParams(String query) throws IOException {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) return params;
        String[] pairs = query.split("&");
        for (String p : pairs) {
            int idx = p.indexOf('=');
            if (idx > 0) {
                String k = URLDecoder.decode(p.substring(0, idx), StandardCharsets.UTF_8.name());
                String v = URLDecoder.decode(p.substring(idx + 1), StandardCharsets.UTF_8.name());
                params.put(k, v);
            } else if (!p.isEmpty()) {
                String k = URLDecoder.decode(p, StandardCharsets.UTF_8.name());
                params.put(k, "");
            }
        }
        return params;
    }

    static class SubmitHandler implements HttpHandler {
        private final TaskService taskService;
        SubmitHandler(TaskService taskService) { this.taskService = taskService; }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String body = readRequestBody(exchange); // job JSON
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
            try {
                String result = taskService.submitTask(body, params);
                if (result.startsWith("ERROR:")) {
                    writeJsonResponse(exchange, 429, "{\"error\":\"" + escapeJson(result) + "\"}");
                } else {
                    writeJsonResponse(exchange, 200, "{\"taskId\":\"" + escapeJson(result) + "\"}");
                }
            } catch (Exception e) {
                writeJsonResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    static class StatusHandler implements HttpHandler {
        private final TaskService taskService;
        StatusHandler(TaskService taskService) { this.taskService = taskService; }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String query = exchange.getRequestURI().getQuery();
            String taskId = null;
            if (query != null) {
                for (String kv : query.split("&")) {
                    String[] parts = kv.split("=", 2);
                    if (parts.length == 2 && "taskId".equals(parts[0])) {
                        taskId = parts[1];
                        break;
                    }
                }
            }
            if (taskId == null) {
                writeJsonResponse(exchange, 400, "{\"error\":\"missing taskId\"}");
                return;
            }
            TaskInfo info = taskService.getTaskInfo(taskId);
            if (info == null) {
                writeJsonResponse(exchange, 404, "{\"error\":\"task not found\"}");
                return;
            }
            String json = "{\"taskId\":\"" + escapeJson(info.getTaskId()) + "\"," +
                    "\"status\":\"" + info.getStatus().name() + "\"," +
                    "\"result\":\"" + escapeJson(info.getResult()) + "\"," +
                    "\"error\":\"" + escapeJson(info.getError()) + "\"}";
            writeJsonResponse(exchange, 200, json);
        }
    }

    static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
