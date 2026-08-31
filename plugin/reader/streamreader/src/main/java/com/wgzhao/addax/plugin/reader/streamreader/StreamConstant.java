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

package com.wgzhao.addax.plugin.reader.streamreader;

import com.wgzhao.addax.core.base.Constant;

/** Stream Constant configuration keys. */
public final class StreamConstant extends Constant
{

    /** Random. */
    public static final String RANDOM = "random";

    // 递增字段
    /** Incr. */
    public static final String INCR = "incr";


    /** Have mixup function. */
    public static final String HAVE_MIXUP_FUNCTION = "hasMixupFunction";
    /** Mixup function pattern. */
    public static final String MIXUP_FUNCTION_PATTERN = "\\s*(.*)\\s*,\\s*(.*)\\s*";
    /** Mixup function param1. */
    public static final String MIXUP_FUNCTION_PARAM1 = "mixupParam1";
    /** Mixup function param2. */
    public static final String MIXUP_FUNCTION_PARAM2 = "mixupParam2";
    /** Mixup function scale. */
    public static final String MIXUP_FUNCTION_SCALE = "scale";
    /** Have incr function. */
    public static final String HAVE_INCR_FUNCTION = "hasIncrFunction";

    private StreamConstant() {}
}
