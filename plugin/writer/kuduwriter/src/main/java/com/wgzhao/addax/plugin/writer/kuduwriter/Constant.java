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

package com.wgzhao.addax.plugin.writer.kuduwriter;

public class Constant
{
    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final String COMPRESSION = "DEFAULT_COMPRESSION";
    public static final String ENCODING = "AUTO_ENCODING";
    // unit: second
    public static final Long ADMIN_TIMEOUT = 60L;
    // unit second
    public static final Long SESSION_TIMEOUT = 60L;

    public static final String INSERT_MODE = "upsert";
    public static final long DEFAULT_WRITE_BATCH_SIZE = 512L;
    public static final long DEFAULT_MUTATION_BUFFER_SPACE = 3072L;
}
