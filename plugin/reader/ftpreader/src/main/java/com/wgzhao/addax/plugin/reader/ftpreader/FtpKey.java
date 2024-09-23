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

package com.wgzhao.addax.plugin.reader.ftpreader;

import com.wgzhao.addax.common.base.Key;

public class FtpKey
        extends Key
{
    private FtpKey() {}

    public static final String PROTOCOL = "protocol";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TIME_OUT = "timeout";
    public static final String CONNECT_PATTERN = "connectPattern";
    public static final String MAX_TRAVERSAL_LEVEL = "maxTraversalLevel";

    public static final String USE_KEY = "useKey";
    // ssh private key
    public static final String KEY_PATH = "keyPath";
    // ssh private key passphrase
    public static final String KEY_PASS = "keyPass";
}
