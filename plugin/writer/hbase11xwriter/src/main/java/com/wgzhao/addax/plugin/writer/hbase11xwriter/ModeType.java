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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.wgzhao.addax.common.exception.AddaxException;

import java.util.Arrays;

public enum ModeType
{
    NORMAL("normal"),
    MULTI_VERSION("multiVersion");

    private final String mode;

    ModeType(String mode)
    {
        this.mode = mode.toLowerCase();
    }

    public static ModeType getByTypeName(String modeName)
    {
        for (ModeType modeType : values()) {
            if (modeType.mode.equalsIgnoreCase(modeName)) {
                return modeType;
            }
        }
        throw AddaxException.asAddaxException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                String.format("The mode %s is unsupported. %s are supported yet.", modeName, Arrays.asList(values())));
    }
}
