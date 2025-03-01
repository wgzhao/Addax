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

import com.wgzhao.addax.transformer.ComplexTransformer;


public class TransformerInfo
{
    private ComplexTransformer transformer;
    private ClassLoader classLoader;
    private boolean isNative;

    public ComplexTransformer getTransformer()
    {
        return transformer;
    }

    public void setTransformer(ComplexTransformer transformer)
    {
        this.transformer = transformer;
    }

    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    public boolean isNative()
    {
        return isNative;
    }

    public void setIsNative(boolean isNative)
    {
        this.isNative = isNative;
    }
}
