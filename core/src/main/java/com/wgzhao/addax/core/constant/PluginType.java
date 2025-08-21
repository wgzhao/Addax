/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.constant;

/**
 * Created by jingxing on 14-8-31.
 */
public enum PluginType
{
    // pluginType also represents the resource directory layout. Avoid adding new types unless necessary.
    // Marking Handler here for future discussion (similar to transformer).
    READER("reader"), TRANSFORMER("transformer"), WRITER("writer"), HANDLER("handler");

    private final String pluginType;

    PluginType(String pluginType)
    {
        this.pluginType = pluginType;
    }

    @Override
    public String toString()
    {
        return this.pluginType;
    }
}
