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

package com.wgzhao.addax.plugin.writer.iotdbwriter;

import com.wgzhao.addax.core.base.Key;

/** IoTDB writer specific configuration keys. */
public final class IotdbKey
        extends Key
{
    /** Selects the IoTDB SQL dialect: tree (default) or table. */
    public static final String SQL_DIALECT = "sqlDialect";
    /** Target device path in the tree model, for example root.factory.device1. */
    public static final String DEVICE = "device";
    /** Measurements to write in the tree model, in record order. */
    public static final String MEASUREMENTS = "measurements";
    /** Position of the time column in the incoming record. */
    public static final String TIME_COLUMN_POSITION = "timeColumnPosition";
    /** Optional TSDataType names for the tree model measurements. */
    public static final String COLUMN_TYPE = "columnType";

    private IotdbKey()
    {
    }
}
