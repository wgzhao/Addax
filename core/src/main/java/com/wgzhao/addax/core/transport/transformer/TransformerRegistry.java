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

package com.wgzhao.addax.core.transport.transformer;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.container.JarLoader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.PLUGIN_INIT_ERROR;
import static com.wgzhao.addax.core.util.container.CoreConstant.STORAGE_TRANSFORMER_HOME;

public class TransformerRegistry
{
    private static final Logger LOG = LoggerFactory.getLogger(TransformerRegistry.class);

    private static final Map<String, TransformerInfo> registeredTransformer = new ConcurrentHashMap<>();

    /**
     * Load the user transformer directories that the job references. Loading is
     * idempotent: a transformer already in the registry is skipped, so the repeated
     * calls made by the task executors do not re-instantiate user classes.
     *
     * @param transformers the transformer names the job needs, or null to load all
     */
    public static synchronized void loadTransformerFromLocalStorage(List<String> transformers)
    {
        if (transformers != null && transformers.stream().allMatch(registeredTransformer::containsKey)) {
            return;
        }

        String[] paths = new File(STORAGE_TRANSFORMER_HOME).list();
        if (null == paths) {
            LOG.debug("The transformer storage [{}] does not exist or is not readable.", STORAGE_TRANSFORMER_HOME);
            return;
        }

        for (String each : paths) {
            if (transformers != null && !transformers.contains(each)) {
                continue;
            }
            if (registeredTransformer.containsKey(each)) {
                continue;
            }
            try {
                loadTransformer(each);
            }
            catch (Exception e) {
                LOG.error("Skip transformer [{}], loadTransformer has encountered an exception [{}].",
                        each, e.getMessage(), e);
            }
        }
    }

    public static synchronized void loadTransformer(String each)
    {
        String transformerPath = STORAGE_TRANSFORMER_HOME + File.separator + each;
        Configuration transformerConfiguration;
        try {
            transformerConfiguration = loadTransformerConfig(transformerPath);
        }
        catch (Exception e) {
            LOG.error("Skip transformer[{}], load transformer.json error, path = {}, ",
                    each, transformerPath, e);
            return;
        }

        String className = transformerConfiguration.getString("class");
        if (StringUtils.isEmpty(className)) {
            LOG.error("Skip transformer[{}], class not config, path = {}, config = {}",
                    each, transformerPath, transformerConfiguration.beautify());
            return;
        }

        String funName = transformerConfiguration.getString("name");
        if (!each.equals(funName)) {
            LOG.warn("The transformer[{}] name not match transformer.json config name[{}], " +
                            "will ignore json name, path = {}, config = {}",
                    each, funName, transformerPath, transformerConfiguration.beautify());
        }

        try {
            checkName(each, false);
        }
        catch (AddaxException e) {
            LOG.error("Skip transformer[{}]: the user transformer name must not start with dx_, path = {}",
                    each, transformerPath);
            return;
        }

        JarLoader jarLoader = new JarLoader(new String[] {transformerPath});
        boolean registered = false;
        try {
            Class<?> transformerClass = jarLoader.loadClass(className);
            validateTransformerClass(each, className, transformerPath, transformerClass);

            if (ComplexTransformer.class.isAssignableFrom(transformerClass)) {
                Class<? extends ComplexTransformer> clazz = transformerClass.asSubclass(ComplexTransformer.class);
                register(each, jarLoader, false, () -> newComplexTransformer(clazz, each));
            }
            else if (Transformer.class.isAssignableFrom(transformerClass)) {
                Class<? extends Transformer> clazz = transformerClass.asSubclass(Transformer.class);
                register(each, jarLoader, false, () -> newTransformer(clazz, each));
            }
            else {
                LOG.error("Load Transformer class[{}] error, path = {}", className, transformerPath);
                return;
            }
            registered = true;
        }
        catch (Exception e) {
            // error, skip function
            LOG.error("Skip transformer({}), load Transformer class error, path = {} ", each, transformerPath, e);
        }
        finally {
            // keep the loader open for the registry lifetime so classes lazily
            // loaded during evaluation still resolve; close it only on failure
            if (!registered) {
                try {
                    jarLoader.close();
                }
                catch (Exception ignored) {
                    // closing a failed loader is best-effort
                }
            }
        }
    }

    private static Configuration loadTransformerConfig(String transformerPath)
    {
        return Configuration.from(new File(transformerPath, "transformer.json"));
    }

    private static void validateTransformerClass(String name, String className, String transformerPath, Class<?> clazz)
    {
        int modifiers = clazz.getModifiers();
        if (!Modifier.isPublic(modifiers) || Modifier.isAbstract(modifiers)) {
            throw AddaxException.asAddaxException(PLUGIN_INIT_ERROR, String.format(
                    "The transformer [%s] cannot be loaded: the class [%s] in [%s] must be public and non-abstract.",
                    name, className, transformerPath));
        }
        try {
            clazz.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw AddaxException.asAddaxException(PLUGIN_INIT_ERROR, String.format(
                    "The transformer [%s] cannot be loaded: the class [%s] in [%s] must declare a public no-arg constructor.",
                    name, className, transformerPath), e);
        }
    }

    private static ComplexTransformer newTransformer(Class<? extends Transformer> clazz, String name)
    {
        try {
            Transformer transformer = clazz.getConstructor().newInstance();
            transformer.setTransformerName(name);
            return new ComplexTransformerProxy(transformer);
        }
        catch (ReflectiveOperationException e) {
            throw AddaxException.asAddaxException(PLUGIN_INIT_ERROR,
                    "Failed to instantiate the transformer [" + name + "]", e);
        }
    }

    private static ComplexTransformer newComplexTransformer(Class<? extends ComplexTransformer> clazz, String name)
    {
        try {
            ComplexTransformer transformer = clazz.getConstructor().newInstance();
            transformer.setTransformerName(name);
            return transformer;
        }
        catch (ReflectiveOperationException e) {
            throw AddaxException.asAddaxException(PLUGIN_INIT_ERROR,
                    "Failed to instantiate the transformer [" + name + "]", e);
        }
    }

    public static TransformerInfo getTransformer(String transformerName)
    {
        return registeredTransformer.get(transformerName);
    }

    public static synchronized void registryTransformer(Transformer transformer)
    {
        registryTransformer(transformer, null, true);
    }

    public static synchronized void registryTransformer(Transformer transformer,
            ClassLoader classLoader, boolean isNative)
    {
        String name = transformer.getTransformerName();
        checkName(name, isNative);
        Class<? extends Transformer> clazz = transformer.getClass();
        register(name, classLoader, isNative, () -> newTransformer(clazz, name));
    }

    public static synchronized void registryComplexTransformer(ComplexTransformer complexTransformer,
            ClassLoader classLoader, boolean isNative)
    {
        String name = complexTransformer.getTransformerName();
        checkName(name, isNative);
        Class<? extends ComplexTransformer> clazz = complexTransformer.getClass();
        register(name, classLoader, isNative, () -> newComplexTransformer(clazz, name));
    }

    private static void register(String name, ClassLoader classLoader, boolean isNative,
            Supplier<ComplexTransformer> factory)
    {
        if (registeredTransformer.containsKey(name)) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    " name=" + name);
        }

        registeredTransformer.put(name, new TransformerInfo(factory, classLoader, isNative));
    }

    private static void checkName(String functionName, boolean isNative)
    {
        boolean checkResult;
        if (isNative) {
            checkResult = functionName.startsWith("dx_");
        }
        else {
            checkResult = !functionName.startsWith("dx_");
        }

        if (!checkResult) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    " name=" + functionName + ": isNative=" + isNative);
        }
    }

    static {
        /*
         * add native transformer
         * local storage and from server will be delay load.
         */
        registryTransformer(new FilterTransformer());
        registryTransformer(new GroovyTransformer());
        registryTransformer(new MapTransformer());
        registryTransformer(new NullToDefaultTransformer());
        registryTransformer(new PadTransformer());
        registryTransformer(new ReplaceTransformer());
        registryTransformer(new SubstrTransformer());
    }
}
