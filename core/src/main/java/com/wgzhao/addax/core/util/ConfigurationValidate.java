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

import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.Validate;

/**
 * Created by jingxing on 14-9-16.
 * <p>
 * 对配置文件做整体检查
 */
public class ConfigurationValidate
{

    private ConfigurationValidate() {}

    public static void doValidate(Configuration allConfig)
    {
        Validate.isTrue(allConfig != null, "");

        coreValidate(allConfig);

        pluginValidate(allConfig);

        jobValidate(allConfig);
    }

    private static void coreValidate(Configuration allConfig)
    {
    }

    private static void pluginValidate(Configuration allConfig)
    {
    }

    private static void jobValidate(Configuration allConfig)
    {
    }
}
