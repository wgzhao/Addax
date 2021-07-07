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

import java.lang.reflect.Constructor;

public final class ClassUtil
{

    /**
     * 通过反射构造类对象
     *
     * @param className 反射的类名称
     * @param t 反射类的类型Class对象
     * @param <T> class type
     * @param args 构造参数
     * @return T
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> T instantiate(String className, Class<T> t,
            Object... args)
    {
        try {
            Constructor constructor = Class.forName(className)
                    .getConstructor(ClassUtil.toClassType(args));
            return (T) constructor.newInstance(args);
        }
        catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Class<?>[] toClassType(Object[] args)
    {
        Class<?>[] clazzs = new Class<?>[args.length];

        for (int i = 0, length = args.length; i < length; i++) {
            clazzs[i] = args[i].getClass();
        }

        return clazzs;
    }
}
