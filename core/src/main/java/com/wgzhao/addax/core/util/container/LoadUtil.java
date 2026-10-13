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
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.taskgroup.runner.AbstractRunner;
import com.wgzhao.addax.core.taskgroup.runner.ReaderRunner;
import com.wgzhao.addax.core.taskgroup.runner.WriterRunner;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.PLUGIN_INSTALL_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * The plugin loader: resolves plugin declarations, loads plugin jars in an isolated class loader
 * and instantiates the Job/Task entry classes of reader and writer plugins.
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
        Class<? extends AbstractPlugin> clazz = LoadUtil.resolveEntryClass(pluginType, pluginName, ContainerType.Job);

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
        Class<? extends AbstractPlugin> clazz = LoadUtil.resolveEntryClass(pluginType, pluginName, ContainerType.Task);

        try {
            AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz.getConstructor().newInstance();
            taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return taskPlugin;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    String.format("Exception occurred when load task plugin [%s].", pluginName), e);
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

    /**
     * Resolves and validates the Job and the Task entry class of a plugin without instantiating
     * either of them, so that a plugin whose entry classes are missing or of the wrong type is
     * rejected when the job starts instead of when some task happens to load it at runtime.
     */
    public static void validatePlugin(PluginType pluginType, String pluginName)
    {
        resolveEntryClass(pluginType, pluginName, ContainerType.Job);
        resolveEntryClass(pluginType, pluginName, ContainerType.Task);
    }

    /**
     * The single place where the entry class of a plugin is derived: the declaration names the
     * class that must expose the nested Job/Task classes, and the framework instantiates exactly
     * the class this method returns.
     */
    private static synchronized Class<? extends AbstractPlugin> resolveEntryClass(
            PluginType pluginType, String pluginName, ContainerType containerType)
    {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        String pluginJsonPath = pluginConf.getString("path") + File.separator + "plugin.json";
        String declaredClass = pluginConf.getString("class");
        if (StringUtils.isBlank(declaredClass)) {
            throw AddaxException.asAddaxException(PLUGIN_INSTALL_ERROR,
                    String.format("The %s plugin [%s] cannot be loaded: the item [class] is missing in [%s].",
                            pluginType, pluginName, pluginJsonPath));
        }

        String entryClassName = declaredClass + "$" + containerType.value();
        Class<?> clazz;
        try {
            clazz = jarLoader.loadClass(entryClassName);
        }
        catch (ClassNotFoundException e) {
            throw AddaxException.asAddaxException(PLUGIN_INSTALL_ERROR, String.format(
                    "The %s plugin [%s] cannot be loaded: the class [%s] declared in [%s] must expose a public static nested class [%s], "
                            + "but the entry class [%s] was not found.",
                    pluginType, pluginName, declaredClass, pluginJsonPath, containerType.value(), entryClassName), e);
        }

        validateEntryClass(pluginType, pluginName, pluginJsonPath, entryClassName, containerType, clazz);
        return clazz.asSubclass(AbstractPlugin.class);
    }

    private static void validateEntryClass(PluginType pluginType, String pluginName, String pluginJsonPath,
            String entryClassName, ContainerType containerType, Class<?> clazz)
    {
        Class<?> expectedBase = expectedBaseClass(pluginType, containerType);
        if (!expectedBase.isAssignableFrom(clazz)) {
            throw AddaxException.asAddaxException(PLUGIN_INSTALL_ERROR, String.format(
                    "The %s plugin [%s] cannot be loaded: the entry class [%s] declared in [%s] must extend [%s].",
                    pluginType, pluginName, entryClassName, pluginJsonPath, expectedBase.getName()));
        }

        int modifiers = clazz.getModifiers();
        if (!Modifier.isPublic(modifiers) || Modifier.isAbstract(modifiers)
                || (clazz.getEnclosingClass() != null && !Modifier.isStatic(modifiers))) {
            throw AddaxException.asAddaxException(PLUGIN_INSTALL_ERROR, String.format(
                    "The %s plugin [%s] cannot be loaded: the entry class [%s] declared in [%s] must be a public, non-abstract class; "
                            + "a nested class must also be static.",
                    pluginType, pluginName, entryClassName, pluginJsonPath));
        }

        try {
            clazz.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw AddaxException.asAddaxException(PLUGIN_INSTALL_ERROR, String.format(
                    "The %s plugin [%s] cannot be loaded: the entry class [%s] declared in [%s] must declare a public no-arg constructor.",
                    pluginType, pluginName, entryClassName, pluginJsonPath), e);
        }
    }

    /**
     * The SPI type an entry class must extend for the given plugin type and container side.
     */
    private static Class<?> expectedBaseClass(PluginType pluginType, ContainerType containerType)
    {
        return switch (pluginType) {
            case READER -> containerType == ContainerType.Job ? Reader.Job.class : Reader.Task.class;
            case WRITER -> containerType == ContainerType.Job ? Writer.Job.class : Writer.Task.class;
            default -> throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    String.format("The plugin type must be reader or writer, [%s] is unsupported.", pluginType));
        };
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
