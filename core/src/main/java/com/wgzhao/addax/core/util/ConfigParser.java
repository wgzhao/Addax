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

import com.wgzhao.addax.core.exception.AddaxException;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.PLUGIN_INIT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
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

public final class ConfigParser
{
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParser.class);

    private ConfigParser()
    {
    }

    /**
     * Parse the job configuration file and merge the core configuration
     *
     * @param jobPath the path of the job configuration file
     * @return the merged configuration
     */
    public static Configuration parse(String jobPath)
    {
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
        configuration.merge(parsePluginConfig(new ArrayList<>(pluginList)), false);

        return configuration;
    }

    /**
     * Upgrade the new job format to the old one
     * 1. The content of job.json is a map instead of list of a map
     *
     * @param configuration {@link Configuration}
     */
    private static void upgradeJobConfig(Configuration configuration)
    {
        configuration.getNecessaryValue(JOB_CONTENT);
        if (configuration.getString(JOB_CONTENT).startsWith("[")) {
            // get the first element
            List<Map> contentList = configuration.getList(JOB_CONTENT, Map.class);
            if (contentList != null && !contentList.isEmpty()) {
                configuration.set("job.content", contentList.get(0));
            }
        }
        Configuration reader = configuration.getConfiguration(JOB_CONTENT_READER_PARAMETER);
        if (reader != null) {
            if (reader.getString(CONNECTION, "").startsWith("[")) {
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
        // switch `write.connection` from list to map
        Configuration writer = configuration.getConfiguration(JOB_CONTENT_WRITER_PARAMETER);
        if (writer != null) {
            if (writer.getString(CONNECTION, "").startsWith("[")) {
                List<Map> connectionList = configuration.getList(JOB_CONTENT_WRITER_PARAMETER + ".connection", Map.class);
                if (connectionList != null && !connectionList.isEmpty()) {
                    writer.set(CONNECTION, connectionList.get(0));
                }
            }
            configuration.set(JOB_CONTENT_WRITER_PARAMETER, writer);
        }
    }

    private static Configuration parseCoreConfig()
    {

        Configuration coreConfig = Configuration.from(new File(CONF_PATH));
        // apply the environment variables
        coreConfig.getMap("entry.environment").forEach((k, v) -> {
            System.setProperty(k, v.toString());
        });
        return coreConfig;
    }

    public static Configuration parseJobConfig(String path)
    {
        String jobContent = getJobContent(path);
        return Configuration.from(jobContent);
    }

    private static String getJobContent(String jobResource)
    {
        String jobContent;

        try {
            jobContent = FileUtils.readFileToString(new File(jobResource), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to obtain job configuration:" + jobResource, e);
        }

        if (jobContent == null) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to obtain job configuration:" + jobResource);
        }
        return jobContent;
    }

    public static Configuration parsePluginConfig(List<String> wantPluginNames)
    {
        Configuration configuration = Configuration.newDefault();

        int complete = 0;
        String pluginType;
        String pluginPath;
        for (String plugin : wantPluginNames) {
            if (plugin.endsWith("reader")) {
                pluginType = "reader";
                pluginPath = PLUGIN_READER_HOME + File.separator + plugin;
            }
            else {
                pluginType = "writer";
                pluginPath = PLUGIN_WRITER_HOME + File.separator + plugin;
            }

            String filePath = pluginPath + File.separator + "plugin.json";
            // check if the plugin.json file exists
            File file = new File(filePath);
            if (!file.exists()) {
                throw AddaxException.asAddaxException(PLUGIN_INIT_ERROR, "The plugin '" + plugin + "' has not installed yet");
            }
            Configuration pluginConf = Configuration.from(file);
            if (StringUtils.isBlank(pluginConf.getString("path"))) {
                pluginConf.set("path", pluginPath);
            }
            Configuration result = Configuration.newDefault();
            result.set(String.format("plugin.%s.%s", pluginType, plugin), pluginConf.getInternal());
            configuration.merge(result, true);
            complete += 1;
        }

        if (!wantPluginNames.isEmpty() && wantPluginNames.size() != complete) {
            throw AddaxException.asAddaxException(PLUGIN_INIT_ERROR, "Plugin loading failed. The specified plugin was not loaded: " + wantPluginNames);
        }

        return configuration;
    }

    private static void validateJob(Configuration conf)
    {
        final Map content = conf.getMap(JOB_CONTENT);
        String[] validPaths = new String[] {JOB_CONTENT_READER, JOB_CONTENT_WRITER, JOB_CONTENT_READER_NAME,
                JOB_CONTENT_READER_PARAMETER, JOB_CONTENT_WRITER_NAME, JOB_CONTENT_WRITER_PARAMETER};

        if (content == null || content.isEmpty()) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE,
                    "The configuration item '" + JOB_CONTENT + "' is required");
        }

        for (String path : validPaths) {
            if (conf.get(path) == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The configuration item '" + path + "' is required");
            }
        }
    }
}
