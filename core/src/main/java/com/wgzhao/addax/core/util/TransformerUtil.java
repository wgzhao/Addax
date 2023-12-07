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
import com.wgzhao.addax.core.transport.transformer.TransformerErrorCode;
import com.wgzhao.addax.core.transport.transformer.TransformerExecution;
import com.wgzhao.addax.core.transport.transformer.TransformerExecutionParas;
import com.wgzhao.addax.core.transport.transformer.TransformerInfo;
import com.wgzhao.addax.core.transport.transformer.TransformerRegistry;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/9.
 */
public class TransformerUtil
{

    private static final Logger LOG = LoggerFactory.getLogger(TransformerUtil.class);

    public static List<TransformerExecution> buildTransformerInfo(Configuration taskConfig)
    {
        List<Configuration> tfConfigs = taskConfig.getListConfiguration(CoreConstant.JOB_TRANSFORMER);
        if (tfConfigs == null || tfConfigs.isEmpty()) {
            return null;
        }

        List<TransformerExecution> result = new ArrayList<>();

        List<String> functionNames = new ArrayList<>();

        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            if (StringUtils.isEmpty(functionName)) {
                throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR,
                        "config=" + configuration.toJSON());
            }

            if ("dx_groovy".equals(functionName) && functionNames.contains("dx_groovy")) {
                throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR,
                        "dx_groovy can be invoke once only.");
            }
            functionNames.add(functionName);
        }

        /*
         * 延迟load 第三方插件的function，并按需load
         */
        LOG.info(String.format("Loading the  user config transformers [%s] ...", functionNames));
        TransformerRegistry.loadTransformerFromLocalStorage(functionNames);

        int i = 0;

        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            TransformerInfo transformerInfo = TransformerRegistry.getTransformer(functionName);
            if (transformerInfo == null) {
                throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_NOTFOUND_ERROR,
                        "name=" + functionName);
            }

            /*
             * 具体的UDF对应一个paras
             */
            TransformerExecutionParas transformerExecutionParas = new TransformerExecutionParas();
            /*
             * groovy function仅仅只有code
             */
            if (!"dx_groovy".equals(functionName) && !"dx_fackGroovy".equals(functionName)) {
                Integer columnIndex = configuration.getInt(CoreConstant.TRANSFORMER_PARAMETER_COLUMN_INDEX);

                if (columnIndex == null) {
                    throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                            "columnIndex must be set by UDF: name=" + functionName);
                }

                transformerExecutionParas.setColumnIndex(columnIndex);
                List<String> paras = configuration.getList(CoreConstant.TRANSFORMER_PARAMETER_PARAS, String.class);
                if (paras != null && !paras.isEmpty()) {
                    transformerExecutionParas.setParas(paras.toArray(new String[0]));
                }
            }
            else {
                String code = configuration.getString(CoreConstant.TRANSFORMER_PARAMETER_CODE);
                String codeFile = configuration.getString(CoreConstant.TRANSFORMER_PARAMETER_CODE_FILE);
                if (StringUtils.isAllEmpty(code, codeFile)) {
                    throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                            "groovy code or codeFile must be set by UDF: name=" + functionName);
                }
                // code and codeFile both setup, prefers to code , ignore codeFile
                if ( StringUtils.isNoneEmpty(code, codeFile)) {
                    LOG.warn("Both setup code and codeFile, will pickup code, ignore the codeFile");
                }
                if (StringUtils.isNotEmpty(codeFile)) {
                    // load groovy code from codeFile
                    // the codeFile default relative path is the same of addax.home properties
                    File file = new File(codeFile);
                    if (! file.exists() || ! file.isFile()) {
                        throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR,
                                "the codeFile [" + codeFile + "]does not exists or is unreadable!");
                    }
                    try {
                        code = FileUtils.readFileToString(file, Charset.defaultCharset());
                    } catch (IOException e) {
                        throw AddaxException.asAddaxException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION,
                                "read codeFile [" + codeFile + "] failure:", e);
                    }
                }
                transformerExecutionParas.setCode(code);

                List<String> extraPackage = configuration.getList(CoreConstant.TRANSFORMER_PARAMETER_EXTRA_PACKAGE, String.class);
                if (extraPackage != null && !extraPackage.isEmpty()) {
                    transformerExecutionParas.setExtraPackage(extraPackage);
                }
            }
            transformerExecutionParas.settContext(configuration.getMap(CoreConstant.TRANSFORMER_PARAMETER_CONTEXT)
            );

            TransformerExecution transformerExecution = new TransformerExecution(transformerInfo,
                    transformerExecutionParas);

            transformerExecution.genFinalParas();
            result.add(transformerExecution);
            i++;
            LOG.info(String.format(" %s of transformer init success. name=%s, isNative=%s parameter = %s"
                    , i, transformerInfo.getTransformer().getTransformerName()
                    , transformerInfo.isNative(), configuration.getConfiguration("parameter")));
        }

        return result;
    }
}
