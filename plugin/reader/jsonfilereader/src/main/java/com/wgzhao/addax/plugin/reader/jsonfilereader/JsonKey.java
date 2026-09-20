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

package com.wgzhao.addax.plugin.reader.jsonfilereader;

import com.wgzhao.addax.core.base.Key;

/** The configuration items of the jsonfilereader plugin. */
public final class JsonKey
        extends Key
{
    /**
     * Whether every record is stored on a line of its own.
     * <p>
     * A value of true (the default) means the file is in the JSON Lines layout, false means the whole
     * file holds one json document and every index matches a json array.
     */
    public static final String SINGLE_LINE = "singleLine";

    private JsonKey() {}
}
