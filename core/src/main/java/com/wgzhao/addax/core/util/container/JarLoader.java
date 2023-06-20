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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 提供Jar隔离的加载机制，会把传入的路径、及其子路径、以及路径中的jar文件加入到class path。
 */
public class JarLoader
        extends URLClassLoader
{
    public JarLoader(String[] paths)
    {
        this(paths, JarLoader.class.getClassLoader());
    }

    public JarLoader(String[] paths, ClassLoader parent)
    {
        super(getURLs(paths), parent);
    }

    private static URL[] getURLs(String[] paths)
    {
        Validate.isTrue(null != paths && 0 != paths.length, "The jar path can not be empty");

        List<String> dirs = new ArrayList<>();
        for (String path : paths) {
            dirs.add(path);
            JarLoader.collectDirs(path, dirs);
        }

        List<URL> urls = new ArrayList<>();
        for (String path : dirs) {
            urls.addAll(doGetURLs(path));
        }

        return urls.toArray(new URL[0]);
    }

    private static void collectDirs(String path, List<String> collector)
    {
        if (null == path || StringUtils.isBlank(path)) {
            return;
        }

        File current = new File(path);
        if (!current.exists() || !current.isDirectory()) {
            return;
        }

        for (File child : Objects.requireNonNull(current.listFiles())) {
            if (!child.isDirectory()) {
                continue;
            }

            collector.add(child.getAbsolutePath());
            collectDirs(child.getAbsolutePath(), collector);
        }
    }

    private static List<URL> doGetURLs(final String path)
    {
        Validate.isTrue(!StringUtils.isBlank(path), "The jar path can not be empty");

        File jarPath = new File(path);

        Validate.isTrue(jarPath.exists() && jarPath.isDirectory(),
                "The jar package path must be exists and it's directory.jar");

        /* set filter */
        FileFilter jarFilter = pathname -> pathname.getName().endsWith(".jar");

        /* iterate all jar */
        File[] allJars = new File(path).listFiles(jarFilter);
        assert allJars != null;
        List<URL> jarURLs = new ArrayList<>(allJars.length);

        for (File allJar : allJars) {
            try {
                jarURLs.add(allJar.toURI().toURL());
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(FrameworkErrorCode.PLUGIN_INIT_ERROR,
                        "Exception occurred when load the jar package.", e);
            }
        }

        return jarURLs;
    }
}
