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

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Compiles a user supplied Groovy rule into a record transformer.
 *
 * <p>The rule is compiled when the transformer is built, so a syntax error fails the
 * job before any record is read. Compiled classes are cached by source text: every
 * task of a job reuses the class, but gets its own instance.
 */
public class GroovyTransformer
        extends Transformer
{
    private static final Map<RuleKey, Class<? extends Transformer>> RULE_CLASSES = new ConcurrentHashMap<>();

    private Transformer rule;

    public GroovyTransformer()
    {
        setTransformerName("dx_groovy");
    }

    @Override
    protected void doInit(Object[] paras)
    {
        if (paras == null || paras.length < 1 || paras.length > 2) {
            throw configError("requires 1 or 2 parameters: code and an optional extraPackage list");
        }
        if (!(paras[0] instanceof String code) || StringUtils.isBlank(code)) {
            throw configError("the parameter [code] must be a non-empty string");
        }

        @SuppressWarnings("unchecked")
        List<String> extraPackage = paras.length == 2 && paras[1] != null
                ? (List<String>) paras[1]
                : List.of();
        RuleKey key = new RuleKey(code, List.copyOf(extraPackage));
        Class<? extends Transformer> ruleClass = RULE_CLASSES.computeIfAbsent(key, this::compile);

        try {
            this.rule = ruleClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, "Failed to instantiate the Groovy rule", e);
        }
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {
        init(paras);
        return this.rule.evaluate(record);
    }

    private Class<? extends Transformer> compile(RuleKey key)
    {
        GroovyClassLoader loader = new GroovyClassLoader(GroovyTransformer.class.getClassLoader());
        String groovyRule = getGroovyRule(key.code(), key.extraPackage());

        Class<?> groovyClass;
        try {
            groovyClass = loader.parseClass(groovyRule);
        }
        catch (CompilationFailedException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to compile the Groovy rule: " + e.getMessage(), e);
        }

        if (!Transformer.class.isAssignableFrom(groovyClass)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "The Groovy rule must produce a Transformer, but produced " + groovyClass.getName());
        }
        return groovyClass.asSubclass(Transformer.class);
    }

    private String getGroovyRule(String expression, List<String> extraPackagesStrList)
    {
        StringBuilder sb = new StringBuilder();
        for (String extraPackagesStr : extraPackagesStrList) {
            if (StringUtils.isNotEmpty(extraPackagesStr)) {
                sb.append(extraPackagesStr);
            }
        }
        sb.append("""
                import static com.wgzhao.addax.core.transport.transformer.GroovyTransformerStaticUtil.*;
                import com.wgzhao.addax.core.element.*;
                import com.wgzhao.addax.core.exception.AddaxException;
                import com.wgzhao.addax.core.transport.transformer.Transformer;
                import java.util.*;
                public class RULE extends Transformer {
                public Record evaluate(Record record, Object... paras) {
                """);
        sb.append(expression);
        sb.append("}}");

        return sb.toString();
    }

    /** The compiled form only depends on the source text and the extra imports. */
    private record RuleKey(String code, List<String> extraPackage)
    {
    }
}
