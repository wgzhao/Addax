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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.util.container.CoreConstant;
import com.wgzhao.addax.core.util.container.JarLoader;
import com.wgzhao.addax.transformer.ComplexTransformer;
import com.wgzhao.addax.transformer.Transformer;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;

/**
 * no comments.
 * Created by liqiang on 16/3/3.
 */
public class TransformerRegistry
{

    private static final Logger LOG = LoggerFactory.getLogger(TransformerRegistry.class);
    private static final Map<String, TransformerInfo> registeredTransformer = new HashMap<>();

    public static void loadTransformerFromLocalStorage()
    {
        //add local_storage transformer
        loadTransformerFromLocalStorage(null);
    }

    public static void loadTransformerFromLocalStorage(List<String> transformers)
    {

        String[] paths = new File(CoreConstant.STORAGE_TRANSFORMER_HOME).list();
        if (null == paths) {
            return;
        }

        for (String each : paths) {
            try {
                if (transformers == null || transformers.contains(each)) {
                    loadTransformer(each);
                }
            }
            catch (Exception e) {
                LOG.error("Skip transformer [{}] LoadTransformer has encountered an exception [{}].",
                        each, e.getMessage(), e);
            }
        }
    }

    public static void loadTransformer(String each)
    {
        String transformerPath = CoreConstant.STORAGE_TRANSFORMER_HOME + File.separator + each;
        Configuration transformerConfiguration;
        try {
            transformerConfiguration = loadTransFormerConfig(transformerPath);
        }
        catch (Exception e) {
            LOG.error("Skip transformer[{}],load transformer.json error, path = {}, ",
                    each, transformerPath, e);
            return;
        }

        String className = transformerConfiguration.getString("class");
        if (StringUtils.isEmpty(className)) {
            LOG.error("Skip transformer[{}],class not config, path = {}, config = {}",
                    each, transformerPath, transformerConfiguration.beautify());
            return;
        }

        String funName = transformerConfiguration.getString("name");
        if (!each.equals(funName)) {
            LOG.warn("The transformer[{}] name not match transformer.json config name[{}], " +
                            "will ignore json  name, path = {}, config = {}",
                    each, funName, transformerPath, transformerConfiguration.beautify());
        }

        try (JarLoader jarLoader = new JarLoader(new String[] {transformerPath})) {
            Class<?> transformerClass = jarLoader.loadClass(className);
            Object transformer = transformerClass.getConstructor().newInstance();
            if (ComplexTransformer.class.isAssignableFrom(transformer.getClass())) {
                ((ComplexTransformer) transformer).setTransformerName(each);
                registryComplexTransformer((ComplexTransformer) transformer, jarLoader, false);
            }
            else if (Transformer.class.isAssignableFrom(transformer.getClass())) {
                ((Transformer) transformer).setTransformerName(each);
                registryTransformer((Transformer) transformer, jarLoader, false);
            }
            else {
                LOG.error("Load Transformer class[{}] error, path = {}", className, transformerPath);
            }
        }
        catch (Exception e) {
            //error, skip function
            LOG.error("Skip transformer({}),load Transformer class error, path = {} ", each, transformerPath, e);
        }
    }

    private static Configuration loadTransFormerConfig(String transformerPath)
    {
        return Configuration.from(new File(FilenameUtils.getPath(transformerPath) + File.separator + "transformer.json"));
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

        checkName(transformer.getTransformerName(), isNative);

        if (registeredTransformer.containsKey(transformer.getTransformerName())) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    " name=" + transformer.getTransformerName());
        }

        registeredTransformer.put(transformer.getTransformerName(),
                buildTransformerInfo(new ComplexTransformerProxy(transformer),
                        isNative, classLoader));
    }

    public static synchronized void registryComplexTransformer(ComplexTransformer complexTransformer,
            ClassLoader classLoader, boolean isNative)
    {

        checkName(complexTransformer.getTransformerName(), isNative);

        if (registeredTransformer.containsKey(complexTransformer.getTransformerName())) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    " name=" + complexTransformer.getTransformerName());
        }

        registeredTransformer.put(complexTransformer.getTransformerName(),
                buildTransformerInfo(complexTransformer, isNative, classLoader));
    }

    private static void checkName(String functionName, boolean isNative)
    {
        boolean checkResult = true;
        if (isNative) {
            if (!functionName.startsWith("dx_")) {
                checkResult = false;
            }
        }
        else {
            if (functionName.startsWith("dx_")) {
                checkResult = false;
            }
        }

        if (!checkResult) {
            throw AddaxException.asAddaxException(
                    CONFIG_ERROR,
                    " name=" + functionName + ": isNative=" + isNative);
        }
    }

    private static TransformerInfo buildTransformerInfo(ComplexTransformer complexTransformer,
            boolean isNative, ClassLoader classLoader)
    {
        TransformerInfo transformerInfo = new TransformerInfo();
        transformerInfo.setClassLoader(classLoader);
        transformerInfo.setIsNative(isNative);
        transformerInfo.setTransformer(complexTransformer);
        return transformerInfo;
    }

    public static List<String> getAllSupportTransformer()
    {
        return new ArrayList<>(registeredTransformer.keySet());
    }

    static {
        /*
         * add native transformer
         * local storage and from server will be delay load.
         */
        registryTransformer(new FilterTransformer());
        registryTransformer(new GroovyTransformer());
        registryTransformer(new MapTransformer());
        registryTransformer(new PadTransformer());
        registryTransformer(new ReplaceTransformer());
        registryTransformer(new SubstrTransformer());
    }
}
