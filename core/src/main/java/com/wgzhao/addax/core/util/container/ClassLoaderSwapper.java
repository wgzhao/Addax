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

/**
 * Created by jingxing on 14-8-29.
 * <p>
 * 为避免jar冲突，比如hbase可能有多个版本的读写依赖jar包，JobContainer和TaskGroupContainer
 * 就需要脱离当前classLoader去加载这些jar包，执行完成后，又退回到原来classLoader上继续执行接下来的代码
 */
public final class ClassLoaderSwapper
{
    private ClassLoader storeClassLoader = null;

    private ClassLoaderSwapper()
    {
    }

    public static ClassLoaderSwapper newCurrentThreadClassLoaderSwapper()
    {
        return new ClassLoaderSwapper();
    }

    /*
     * 保存当前classLoader，并将当前线程的classLoader设置为所给classLoader
     */
    public void setCurrentThreadClassLoader(ClassLoader classLoader)
    {
        this.storeClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
    }

    /**
     * 将当前线程的类加载器设置为保存的类加载
     */
    public void restoreCurrentThreadClassLoader()
    {
        Thread.currentThread().setContextClassLoader(this.storeClassLoader);
    }
}
