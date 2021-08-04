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

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.transformer.Transformer;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import java.util.Arrays;
import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class GroovyTransformer
        extends Transformer
{
    private Transformer groovyTransformer;

    public GroovyTransformer()
    {
        setTransformerName("dx_groovy");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {

        if (groovyTransformer == null) {
            //全局唯一
            if (paras.length < 1 || paras.length > 2) {
                throw AddaxException.asAddaxException(
                        TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                        "dx_groovy paras must be 1 or 2 . now paras is: " + Arrays.asList(paras));
            }
            synchronized (this) {

                if (groovyTransformer == null) {
                    String code = (String) paras[0];
                    @SuppressWarnings("unchecked") List<String> extraPackage = paras.length == 2 ?
                            (List<String>) paras[1] : null;
                    initGroovyTransformer(code, extraPackage);
                }
            }
        }

        return this.groovyTransformer.evaluate(record);
    }

    private void initGroovyTransformer(String code, List<String> extraPackage)
    {
        GroovyClassLoader loader = new GroovyClassLoader(GroovyTransformer.class.getClassLoader());
        String groovyRule = getGroovyRule(code, extraPackage);

        Class groovyClass;
        try {
            groovyClass = loader.parseClass(groovyRule);
        }
        catch (CompilationFailedException cfe) {
            throw AddaxException.asAddaxException(
                    TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, cfe);
        }

        try {
            Object t = groovyClass.newInstance();
            if (!(t instanceof Transformer)) {
                throw AddaxException.asAddaxException(
                        TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION,
                        "Addax bug! ");
            }
            this.groovyTransformer = (Transformer) t;
        }
        catch (Throwable ex) {
            throw AddaxException.asAddaxException(
                    TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION, ex);
        }
    }

    private String getGroovyRule(String expression, List<String> extraPackagesStrList)
    {
        StringBuilder sb = new StringBuilder();
        if (extraPackagesStrList != null) {
            for (String extraPackagesStr : extraPackagesStrList) {
                if (StringUtils.isNotEmpty(extraPackagesStr)) {
                    sb.append(extraPackagesStr);
                }
            }
        }
        sb.append("import static com.wgzhao.addax.core.transport.transformer.GroovyTransformerStaticUtil.*;");
        sb.append("import com.wgzhao.addax.common.element.*;");
        sb.append("import com.wgzhao.addax.common.exception.AddaxException;");
        sb.append("import com.wgzhao.addax.transformer.Transformer;");
        sb.append("import java.util.*;");
        sb.append("public class RULE extends Transformer").append("{");
        sb.append("public Record evaluate(Record record, Object... paras) {");
        sb.append(expression);
        sb.append("}}");

        return sb.toString();
    }
}
