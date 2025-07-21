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

package com.wgzhao.addax.core.util.container;

import com.wgzhao.addax.core.constant.PluginType;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.AbstractJobPlugin;
import com.wgzhao.addax.core.plugin.AbstractPlugin;
import com.wgzhao.addax.core.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.taskgroup.runner.AbstractRunner;
import com.wgzhao.addax.core.taskgroup.runner.ReaderRunner;
import com.wgzhao.addax.core.taskgroup.runner.WriterRunner;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.PLUGIN_INSTALL_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * The plugin loader, which is roughly divided into three types of plugins: reader, transformer (not yet implemented), and writer.
 */
public class LoadUtil
{
    private static Configuration configurationSet;

    private static final Map<String, JarLoader> jarLoaderCenter = new HashMap<>();

    private LoadUtil()
    {
    }


    public static synchronized void bind(Configuration pluginConfigs)
    {
        configurationSet = pluginConfigs;
    }

    private static String generatePluginKey(PluginType pluginType, String pluginName)
    {
        return String.format("plugin.%s.%s", pluginType.toString(), pluginName);
    }

    private static Configuration getPluginConf(PluginType pluginType, String pluginName)
    {
        Configuration pluginConf = configurationSet.getConfiguration(generatePluginKey(pluginType, pluginName));

        if (null == pluginConf) {
            throw AddaxException.asAddaxException(
                    PLUGIN_INSTALL_ERROR,
                    String.format("Can not find the configure of plugin [%s].", pluginName));
        }

        return pluginConf;
    }

    public static AbstractJobPlugin loadJobPlugin(PluginType pluginType, String pluginName)
    {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName, ContainerType.Job);

        try {
            AbstractJobPlugin jobPlugin = (AbstractJobPlugin) clazz.getConstructor().newInstance();
            jobPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return jobPlugin;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR,
                    String.format("Exception occurred when load job plugin [%s].", pluginName), e);
        }
    }

    public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType, String pluginName)
    {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName, ContainerType.Task);

        try {
            AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz.getConstructor().newInstance();
            taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return taskPlugin;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    String.format("Can not find the configure of task plugin [%s].", pluginName), e);
        }
    }

    public static AbstractRunner loadPluginRunner(PluginType pluginType, String pluginName)
    {
        AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(pluginType, pluginName);

        return switch (pluginType) {
            case READER -> new ReaderRunner(taskPlugin);
            case WRITER -> new WriterRunner(taskPlugin);
            default -> throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    String.format("The plugin type must be reader or writer, [%s] is unsupported.", pluginName));
        };
    }

    @SuppressWarnings("unchecked")
    private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
            PluginType pluginType, String pluginName,
            ContainerType pluginRunType)
    {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        try {
            return (Class<? extends AbstractPlugin>) jarLoader.loadClass(pluginConf.getString("class") + "$"
                    + pluginRunType.value());
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    public static synchronized JarLoader getJarLoader(PluginType pluginType, String pluginName)
    {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType, pluginName));
        if (null == jarLoader) {
            String pluginPath = pluginConf.getString("path");
            if (StringUtils.isBlank(pluginPath)) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR,
                        String.format("Illegal path of plugin [%s] for [%s].",  pluginName, pluginType));
            }
            jarLoader = new JarLoader(new String[] {pluginPath});
            jarLoaderCenter.put(generatePluginKey(pluginType, pluginName), jarLoader);
        }

        return jarLoader;
    }

    private enum ContainerType
    {
        Job("Job"), Task("Task");
        private final String type;

        ContainerType(String type)
        {
            this.type = type;
        }

        public String value()
        {
            return type;
        }
    }
}
