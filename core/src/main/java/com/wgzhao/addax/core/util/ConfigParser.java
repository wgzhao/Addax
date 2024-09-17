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

package com.wgzhao.addax.core.util;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.common.base.Key.CONNECTION;
import static com.wgzhao.addax.core.util.container.CoreConstant.CONF_PATH;
import static com.wgzhao.addax.core.util.container.CoreConstant.CORE_SERVER_TIMEOUT_SEC;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_READER;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_READER_NAME;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_READER_PARAMETER;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_READER_PARAMETER_CONNECTION;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_WRITER;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_WRITER_NAME;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_CONTENT_WRITER_PARAMETER;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_POST_HANDLER_PLUGIN_NAME;
import static com.wgzhao.addax.core.util.container.CoreConstant.JOB_PRE_HANDLER_PLUGIN_NAME;
import static com.wgzhao.addax.core.util.container.CoreConstant.PLUGIN_READER_HOME;
import static com.wgzhao.addax.core.util.container.CoreConstant.PLUGIN_WRITER_HOME;


public final class ConfigParser {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParser.class);

    private ConfigParser() {
    }

    /*
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     */
    public static Configuration parse(String jobPath) {
        Configuration configuration = ConfigParser.parseJobConfig(jobPath);

        // Upgrade the new job format to the old one
        upgradeJobConfig(configuration);
        //validate job json
        validateJob(configuration);

        configuration.merge(ConfigParser.parseCoreConfig(), false);
        String readerPluginName = configuration.getString(JOB_CONTENT_READER_NAME);
        String writerPluginName = configuration.getString(JOB_CONTENT_WRITER_NAME);

        String preHandlerName = configuration.getString(JOB_PRE_HANDLER_PLUGIN_NAME);

        String postHandlerName = configuration.getString(JOB_POST_HANDLER_PLUGIN_NAME);

        Set<String> pluginList = new HashSet<>();
        pluginList.add(readerPluginName);
        pluginList.add(writerPluginName);

        if (StringUtils.isNotEmpty(preHandlerName)) {
            pluginList.add(preHandlerName);
        }
        if (StringUtils.isNotEmpty(postHandlerName)) {
            pluginList.add(postHandlerName);
        }
        try {
            configuration.merge(parsePluginConfig(new ArrayList<>(pluginList)), false);
        } catch (Exception e) {
            //吞掉异常，保持log干净。这里message足够。
            LOG.warn("Failed to load plugin(s) [{},{}]: {}, try after 1 second.", readerPluginName, writerPluginName, e.getMessage());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                //
            }
            configuration.merge(parsePluginConfig(new ArrayList<>(pluginList)), false);
        }

        return configuration;
    }

    /**
     * Upgrade the new job format to the old one
     * 1. the content of job.json is a map instead of list of map
     *
     * @param configuration {@link Configuration}
     */
    private static void upgradeJobConfig(Configuration configuration) {
        if (configuration.getString(JOB_CONTENT).startsWith("[")) {
            // get the first element
            List<Map> contentList = configuration.getList(JOB_CONTENT, Map.class);
            if (contentList != null && !contentList.isEmpty()) {
                configuration.set("job.content", contentList.get(0));
            }
        }
        Configuration reader = configuration.getConfiguration(JOB_CONTENT_READER_PARAMETER);
        if (reader != null) {
            if (reader.getString(CONNECTION).startsWith("[")) {
                List<Map> connectionList = configuration.getList(JOB_CONTENT_READER_PARAMETER_CONNECTION, Map.class);
                if (connectionList != null && !connectionList.isEmpty()) {
                    reader.set(CONNECTION, connectionList.get(0));
                }
            }
            if (reader.getString("connection.jdbcUrl", "").startsWith("[")) {
                reader.set("connection.jdbcUrl", reader.getList("connection.jdbcUrl", String.class).get(0));
            }
            configuration.set(JOB_CONTENT_READER_PARAMETER, reader);
        }
    }

    private static Configuration parseCoreConfig() {
        return Configuration.from(new File(CONF_PATH));
    }

    public static Configuration parseJobConfig(String path) {
        String jobContent = getJobContent(path);
        return Configuration.from(jobContent);
    }

    private static String getJobContent(String jobResource) {
        String jobContent;

        boolean isJobResourceFromHttp = jobResource.trim().toLowerCase().startsWith("http");

        if (isJobResourceFromHttp) {
            //设置httpclient的 HTTP_TIMEOUT_IN_MILLION_SECONDS
            Configuration coreConfig = ConfigParser.parseCoreConfig();
            int httpTimeOutInMillionSeconds = coreConfig.getInt(CORE_SERVER_TIMEOUT_SEC, 5) * 1000;
            HttpClientUtil.setHttpTimeoutInMillionSeconds(httpTimeOutInMillionSeconds);

            HttpClientUtil httpClientUtil = new HttpClientUtil();
            try {
                URL url = new URL(jobResource);
                HttpGet httpGet = HttpClientUtil.getGetRequest();
                httpGet.setURI(url.toURI());

                jobContent = httpClientUtil.executeAndGetWithFailedRetry(httpGet, 1, 1000L);
            } catch (Exception e) {
                throw AddaxException.asAddaxException(FrameworkErrorCode.CONFIG_ERROR, "Failed to obtain job configuration:" + jobResource, e);
            }
        } else {
            // jobResource 是本地文件绝对路径
            try {
                jobContent = FileUtils.readFileToString(new File(jobResource), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw AddaxException.asAddaxException(FrameworkErrorCode.CONFIG_ERROR, "Failed to obtain job configuration:" + jobResource, e);
            }
        }

        if (jobContent == null) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.CONFIG_ERROR, "Failed to obtain job configuration:" + jobResource);
        }
        return jobContent;
    }

    public static Configuration parsePluginConfig(List<String> wantPluginNames) {
        Configuration configuration = Configuration.newDefault();

        int complete = 0;
        String pluginType;
        String pluginPath;
        for (String plugin : wantPluginNames) {
            if (plugin.endsWith("reader")) {
                pluginType = "reader";
                pluginPath = PLUGIN_READER_HOME + File.separator + plugin;
            } else {
                pluginType = "writer";
                pluginPath = PLUGIN_WRITER_HOME + File.separator + plugin;
            }

            String filePath = pluginPath + File.separator + "plugin.json";
            Configuration pluginConf = Configuration.from(new File(filePath));
            if (StringUtils.isBlank(pluginConf.getString("path"))) {
                pluginConf.set("path", pluginPath);
            }
            Configuration result = Configuration.newDefault();
            result.set(String.format("plugin.%s.%s", pluginType, plugin), pluginConf.getInternal());
            configuration.merge(result, true);
            complete += 1;
        }

        if (!wantPluginNames.isEmpty() && wantPluginNames.size() != complete) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.PLUGIN_INIT_ERROR, "Plugin loading failed. The specified plugin was not loaded: " + wantPluginNames);
        }

        return configuration;
    }

    private static void validateJob(Configuration conf) {
        final Map content = conf.getMap(JOB_CONTENT);
        String[] validPaths = new String[]{JOB_CONTENT_READER, JOB_CONTENT_WRITER, JOB_CONTENT_READER_NAME,
                JOB_CONTENT_READER_PARAMETER, JOB_CONTENT_WRITER_NAME, JOB_CONTENT_WRITER_PARAMETER};

        if (content == null || content.isEmpty()) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR,
                    "The configuration item '" + JOB_CONTENT + "' is required");
        }

        for (String path : validPaths) {
            if (content.get(path) == null) {
                throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR,
                        "The configuration item '" + path + "' is required");
            }
        }
    }
}
