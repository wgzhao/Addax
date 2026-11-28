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

package com.wgzhao.addax.plugin.reader.iotdbreader;

import com.wgzhao.addax.core.base.Key;

/** IoTDB reader specific configuration keys. */
public final class IotdbKey
        extends Key
{
    /** Selects the IoTDB SQL dialect: tree (default) or table. */
    public static final String SQL_DIALECT = "sqlDialect";
    /** Target device path in the tree model, for example root.factory.device1. */
    public static final String DEVICE = "device";
    /** Measurements to read in the tree model. */
    public static final String MEASUREMENTS = "measurements";
    /** Position of the implicit time column in the output record. */
    public static final String TIME_COLUMN_POSITION = "timeColumnPosition";
    /** Start of a time range split, inclusive. */
    public static final String BEGIN_DATETIME = "beginDateTime";
    /** End of a time range split, exclusive. */
    public static final String END_DATETIME = "endDateTime";
    /** Size of each time range split, for example 1d, 6h or 30m. */
    public static final String SPLIT_INTERVAL = "splitInterval";

    private IotdbKey()
    {
    }
}
