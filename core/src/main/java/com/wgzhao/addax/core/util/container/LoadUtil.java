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

import com.wgzhao.addax.common.constant.PluginType;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.AbstractJobPlugin;
import com.wgzhao.addax.common.plugin.AbstractPlugin;
import com.wgzhao.addax.common.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.taskgroup.runner.AbstractRunner;
import com.wgzhao.addax.core.taskgroup.runner.ReaderRunner;
import com.wgzhao.addax.core.taskgroup.runner.WriterRunner;
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jingxing on 14-8-24.
 * <p>
 * 插件加载器，大体上分reader、transformer（还未实现）和writer三中插件类型，
 * reader和writer在执行时又可能出现Job和Task两种运行时（加载的类不同）
 */
public class LoadUtil
{
    private static Configuration configurationSet;

    /*
     * jarLoader的缓冲
     */
    private static final Map<String, JarLoader> jarLoaderCenter = new HashMap<>();

    private LoadUtil()
    {
    }

    /*
     * 设置pluginConfigs，方便后面插件来获取
     */
    public static synchronized void bind(Configuration pluginConfigs)
    {
        /*
         * 所有插件配置放置在pluginRegisterCenter中，为区别reader、transformer和writer，还能区别
         * 具体pluginName，故使用pluginType.pluginName作为key放置在该map中
         */
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
                    FrameworkErrorCode.PLUGIN_INSTALL_ERROR,
                    String.format("不能找到插件[%s]的配置.", pluginName));
        }

        return pluginConf;
    }

    /*
     * 加载JobPlugin，reader、writer都可能要加载
     */
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
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("找到plugin[%s]的Job配置.", pluginName), e);
        }
    }

    /*
     * 加载taskPlugin，reader、writer都可能加载
     */
    public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType, String pluginName)
    {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName, ContainerType.Task);

        try {
            AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz.getConstructor().newInstance();
            taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return taskPlugin;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("不能找plugin[%s]的Task配置.", pluginName), e);
        }
    }

    /*
     * 根据插件类型、名字和执行时taskGroupId加载对应运行器
     */
    public static AbstractRunner loadPluginRunner(PluginType pluginType, String pluginName)
    {
        AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(pluginType, pluginName);

        switch (pluginType) {
            case READER:
                return new ReaderRunner(taskPlugin);
            case WRITER:
                return new WriterRunner(taskPlugin);
            default:
                throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR,
                        String.format("插件[%s]的类型必须是[reader]或[writer]!", pluginName));
        }
    }

    /*
     * 反射出具体plugin实例
     */
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
            throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    public static synchronized JarLoader getJarLoader(PluginType pluginType, String pluginName)
    {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType, pluginName));
        if (null == jarLoader) {
            String pluginPath = pluginConf.getString("path");
            if (StringUtils.isBlank(pluginPath)) {
                throw AddaxException.asAddaxException(FrameworkErrorCode.RUNTIME_ERROR,
                        String.format("%s插件[%s]路径非法!", pluginType, pluginName));
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
