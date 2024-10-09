/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.reader.redisreader;

import com.wgzhao.addax.common.base.Key;

public class RedisKey
    extends Key
{
    public static final String URI = "uri";
    public static final String MODE = "mode";
    public static final String AUTH = "auth";
    public static final String MASTER_NAME  = "masterName";
    public static final String INCLUDE = "include";
    public static final String EXCLUDE = "exclude";
    public static final String DB = "db";
    public static final String KEY_THRESHOLD_LENGTH = "keyThresholdLength";
}
