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

import java.util.function.Supplier;

/**
 * A registered transformer: the factory creates one instance per execution so that
 * parsed parameters and any user state are never shared between tasks.
 */
public class TransformerInfo
{
    private final Supplier<ComplexTransformer> factory;
    private final ClassLoader classLoader;
    private final boolean nativeTransformer;

    public TransformerInfo(Supplier<ComplexTransformer> factory, ClassLoader classLoader, boolean isNative)
    {
        this.factory = factory;
        this.classLoader = classLoader;
        this.nativeTransformer = isNative;
    }

    public ComplexTransformer newTransformer()
    {
        return factory.get();
    }

    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    public boolean isNative()
    {
        return nativeTransformer;
    }
}
