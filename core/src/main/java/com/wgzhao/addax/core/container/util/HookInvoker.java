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

package com.wgzhao.addax.core.container.util;

import com.wgzhao.addax.common.util.Configuration;

import java.io.File;
import java.util.Map;

/**
 * Scan all first-level subdirectories of the given directory, and treat each subdirectory as a directory of Hook.
 * For each subdirectory, it must conform to the standard directory format of ServiceLoader, see
 * <a href="http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html">ServiceLoader</a>.
 * Load the jar inside, and call using the ServiceLoader mechanism.
 */
public class HookInvoker
{

    public HookInvoker(String baseDirName, Configuration conf, Map<String, Number> msg)
    {
        File baseDir = new File(baseDirName);
    }
}
