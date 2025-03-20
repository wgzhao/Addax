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

package com.wgzhao.addax.plugin.writer.influxdbwriter;

import com.wgzhao.addax.core.base.Key;

public final class InfluxDBKey extends Key
{
    public static final String CONNECT_TIMEOUT_SECONDS = "connTimeout";
    public static final String READ_TIMEOUT_SECONDS = "readTimeout";
    public static final String WRITE_TIMEOUT_SECONDS = "writeTimeout";
    public static final String RETENTION_POLICY = "retentionPolicy";
    // retention policy item
    public static final String RP_NAME = "name";
    public static final String RP_DURATION = "duration";
    public static final String RP_REPLICATION = "replication";
}
