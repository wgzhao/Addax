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

import com.wgzhao.addax.core.config.TaskConfig;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.transport.transformer.TransformerExecutionParas;
import com.wgzhao.addax.core.transport.transformer.TransformerInfo;
import com.wgzhao.addax.core.transport.transformer.TransformerRegistry;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/**
 * Builds the transformer executions of a task from the job configuration.
 *
 * <p>Two parameter styles are accepted for the built-in transformers. The named form
 * ({@code {"idx": 1, "pos": 0, "length": 3}}) is the recommended one; the positional
 * form ({@code {"columnIndex": 1, "paras": ["0", "3"]}}) is kept for compatibility and
 * logs a deprecation warning.
 */
public final class TransformerUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(TransformerUtil.class);

    /** Emit the deprecation warning once per transformer name rather than once per task. */
    private static final Set<String> LEGACY_WARNED = ConcurrentHashMap.newKeySet();

    private static final Map<String, NamedSpec> NAMED_SPECS = Map.of(
            "dx_substr", new NamedSpec(List.of("idx", "pos", "length"),
                    "\"idx\": 1, \"pos\": 0, \"length\": 3"),
            "dx_pad", new NamedSpec(List.of("idx", "flag", "length", "chr"),
                    "\"idx\": 1, \"flag\": \"l\", \"length\": 4, \"chr\": \"A\""),
            "dx_replace", new NamedSpec(List.of("idx", "pos", "length", "str"),
                    "\"idx\": 1, \"pos\": 2, \"length\": 4, \"str\": \"****\""),
            "dx_filter", new NamedSpec(List.of("idx", "operator", "expr"),
                    "\"idx\": 1, \"operator\": \">=\", \"expr\": \"10\""),
            "dx_map", new NamedSpec(List.of("idx", "operator", "value"),
                    "\"idx\": 1, \"operator\": \"*\", \"value\": \"2\""));

    private TransformerUtil()
    {
    }

    public static List<TransformerExecution> buildTransformerInfo(TaskConfig taskConfig)
    {
        return build(taskConfig.getTransformers(), false);
    }

    /**
     * Validate the job-level transformer list once, before any task is scheduled.
     * This loads user transformers and parses every parameter set, so configuration
     * errors fail the job instead of turning rows into dirty data.
     *
     * @param transformerConfigs the job-level transformer list
     */
    public static void validateTransformers(List<Configuration> transformerConfigs)
    {
        int count = build(transformerConfigs, true).size();
        if (count > 0) {
            LOG.info("Validated {} transformer configuration(s).", count);
        }
    }

    private static List<TransformerExecution> build(List<Configuration> tfConfigs, boolean validationOnly)
    {
        if (tfConfigs == null || tfConfigs.isEmpty()) {
            return List.of();
        }

        List<String> functionNames = new ArrayList<>(tfConfigs.size());
        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            if (StringUtils.isEmpty(functionName)) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The transformer name is missing: config=" + configuration.toJSON());
            }
            functionNames.add(functionName);
        }

        if (functionNames.stream().filter("dx_groovy"::equals).count() > 1) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "dx_groovy can be invoked once only.");
        }

        if (!validationOnly) {
            LOG.debug("Loading the user configured transformers [{}] ...", functionNames);
        }
        TransformerRegistry.loadTransformerFromLocalStorage(functionNames);

        List<TransformerExecution> result = new ArrayList<>(tfConfigs.size());
        int index = 0;
        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            TransformerInfo transformerInfo = TransformerRegistry.getTransformer(functionName);
            if (transformerInfo == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "transformer name=" + functionName
                                + " (install it under " + CoreConstant.STORAGE_TRANSFORMER_HOME
                                + " or check the configured name)");
            }

            TransformerExecutionParas transformerExecutionParas = parseParameters(configuration, functionName);
            transformerExecutionParas.settContext(
                    configuration.getMap(CoreConstant.TRANSFORMER_PARAMETER_CONTEXT));

            TransformerExecution transformerExecution = new TransformerExecution(transformerInfo,
                    transformerExecutionParas);
            transformerExecution.genFinalParas();
            result.add(transformerExecution);
            index++;

            if (validationOnly) {
                LOG.info(" {} of transformer validated. name={}, isNative={}",
                        index, functionName, transformerInfo.isNative());
            }
            else {
                LOG.debug(" {} of transformer init success. name={}, isNative={}",
                        index, functionName, transformerInfo.isNative());
            }
        }

        return result;
    }

    private static TransformerExecutionParas parseParameters(Configuration configuration, String functionName)
    {
        Configuration parameter = configuration.getConfiguration("parameter");
        if (parameter == null) {
            parameter = Configuration.newDefault();
        }

        TransformerExecutionParas result = new TransformerExecutionParas();

        if ("dx_groovy".equals(functionName)) {
            parseGroovy(parameter, result);
            return result;
        }

        boolean positional = parameter.get("columnIndex") != null || parameter.get("paras") != null;

        if ("dx_null_to_default".equals(functionName)) {
            if (positional) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "dx_null_to_default does not accept columnIndex/paras; configure its column list in parameter.column");
            }
            if (parameter.get("column") == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "dx_null_to_default requires parameter.column, for example: "
                                + "\"parameter\": {\"column\": [{\"name\": \"id\", \"type\": \"string\"}]}");
            }
            result.setParas(new String[] {parameter.toJSON()});
            return result;
        }

        NamedSpec spec = NAMED_SPECS.get(functionName);
        if (spec != null) {
            if (positional) {
                for (String name : spec.names()) {
                    if (parameter.get(name) != null) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR,
                                functionName + " mixes the positional (columnIndex/paras) and named parameter styles; use one of them");
                    }
                }
                warnLegacy(functionName, spec);
                parsePositional(parameter, functionName, result);
            }
            else {
                parseNamed(parameter, functionName, spec, result);
            }
            return result;
        }

        // user-defined transformers still use the positional protocol
        parsePositional(parameter, functionName, result);
        return result;
    }

    private static void parseNamed(Configuration parameter, String functionName, NamedSpec spec,
            TransformerExecutionParas result)
    {
        List<String> names = spec.names();
        result.setColumnIndex(intParameter(parameter, functionName, names.get(0), spec));

        String[] paras = new String[names.size() - 1];
        for (int i = 1; i < names.size(); i++) {
            String name = names.get(i);
            Object value = parameter.get(name);
            if (value == null) {
                throw missingNamedParameter(functionName, name, spec);
            }
            paras[i - 1] = String.valueOf(value);
        }
        result.setParas(paras);
    }

    private static void parsePositional(Configuration parameter, String functionName,
            TransformerExecutionParas result)
    {
        Object rawColumnIndex = parameter.get("columnIndex");
        if (rawColumnIndex == null) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE,
                    "columnIndex must be set by UDF: name=" + functionName);
        }
        int columnIndex;
        try {
            columnIndex = Integer.parseInt(String.valueOf(rawColumnIndex).trim());
        }
        catch (NumberFormatException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    functionName + ": the parameter [columnIndex] must be an integer, but was [" + rawColumnIndex + "]");
        }
        if (columnIndex < 0) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    functionName + ": the parameter [columnIndex] must not be negative");
        }
        result.setColumnIndex(columnIndex);

        Object rawParas = parameter.get("paras");
        if (rawParas == null) {
            return;
        }
        if (!(rawParas instanceof List<?> paras)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    functionName + ": the parameter [paras] must be an array");
        }
        if (!paras.isEmpty()) {
            // values may be JSON numbers; the positional protocol itself is string based
            result.setParas(paras.stream().map(String::valueOf).toArray(String[]::new));
        }
    }

    private static void parseGroovy(Configuration parameter, TransformerExecutionParas result)
    {
        String code = parameter.getString("code");
        String codeFile = parameter.getString("codeFile");
        if (StringUtils.isAllEmpty(code, codeFile)) {
            throw AddaxException.asAddaxException(REQUIRED_VALUE,
                    "groovy code or codeFile must be set by UDF: name=dx_groovy");
        }
        // code and codeFile both setup, prefers to code, ignore codeFile
        if (StringUtils.isNoneEmpty(code, codeFile)) {
            LOG.warn("Both code and codeFile are set, using code and ignoring codeFile");
        }
        if (StringUtils.isNotEmpty(codeFile)) {
            File file = resolveCodeFile(codeFile);
            if (!file.exists() || !file.isFile()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "the codeFile [" + file.getAbsolutePath() + "] does not exist or is unreadable");
            }
            try {
                code = Files.readString(file.toPath());
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR,
                        "read codeFile [" + file.getAbsolutePath() + "] failure:", e);
            }
        }
        result.setCode(code);

        Object rawExtraPackage = parameter.get("extraPackage");
        if (rawExtraPackage != null) {
            if (!(rawExtraPackage instanceof List<?> extraPackage)) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "dx_groovy: the parameter [extraPackage] must be an array");
            }
            if (!extraPackage.isEmpty()) {
                result.setExtraPackage(extraPackage.stream().map(String::valueOf).toList());
            }
        }
    }

    /**
     * A relative codeFile is resolved against the Addax home directory, as documented,
     * instead of the working directory the launcher happens to use.
     */
    private static File resolveCodeFile(String codeFile)
    {
        Path path = Path.of(codeFile);
        if (path.isAbsolute() || CoreConstant.HOME == null) {
            return path.toFile();
        }
        return Path.of(CoreConstant.HOME, codeFile).toFile();
    }

    private static int intParameter(Configuration parameter, String functionName, String name, NamedSpec spec)
    {
        Object value = parameter.get(name);
        if (value == null) {
            throw missingNamedParameter(functionName, name, spec);
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        }
        catch (NumberFormatException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    functionName + ": the parameter [" + name + "] must be an integer, but was [" + value + "]");
        }
    }

    private static AddaxException missingNamedParameter(String functionName, String name, NamedSpec spec)
    {
        return AddaxException.asAddaxException(REQUIRED_VALUE,
                functionName + " requires the parameter [" + name + "]; example: \"parameter\": {" + spec.example() + "}");
    }

    private static void warnLegacy(String functionName, NamedSpec spec)
    {
        if (LEGACY_WARNED.add(functionName)) {
            LOG.warn("The positional transformer parameters (columnIndex/paras) used by [{}] are deprecated "
                            + "and may be removed in a future release; use named parameters instead, "
                            + "for example \"parameter\": {}.",
                    functionName, "{" + spec.example() + "}");
        }
    }

    private record NamedSpec(List<String> names, String example)
    {
    }
}
