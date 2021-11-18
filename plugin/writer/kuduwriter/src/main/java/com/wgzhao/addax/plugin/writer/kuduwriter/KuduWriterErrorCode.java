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

import com.wgzhao.addax.common.spi.ErrorCode;

public enum KuduWriterErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("Kuduwriter-00", "You are missing a required parameter value."),
    ILLEGAL_VALUE("Kuduwriter-01", "You fill in the parameter values are not legitimate."),
    GET_KUDU_CONNECTION_ERROR("Kuduwriter-02", "Error getting Kudu connection."),
    GET_KUDU_TABLE_ERROR("Kuduwriter-03", "Error getting Kudu table."),
    CLOSE_KUDU_CONNECTION_ERROR("Kuduwriter-04", "Error closing Kudu connection."),
    CLOSE_KUDU_SESSION_ERROR("Kuduwriter-06", "Error closing Kudu table connection."),
    PUT_KUDU_ERROR("Kuduwriter-07", "IO exception occurred when writing to Kudu."),
    DELETE_KUDU_ERROR("Kuduwriter-08", "An exception occurred while delete Kudu table."),
    CREATE_KUDU_TABLE_ERROR("Kuduwriter-09", "Error creating Kudu table."),
    PARAMETER_NUM_ERROR("Kuduwriter-10", "The number of parameters does not match."),
    TABLE_NOT_EXISTS("Kuduwriter-11", "The table you specified does not exists yet"),
    COLUMN_NOT_EXISTS("Kuduwriter-12", "the column doest not exists");
    
    private final String code;
    private final String description;

    KuduWriterErrorCode(String code, String description)
    {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode()
    {
        return code;
    }

    @Override
    public String getDescription()
    {
        return description;
    }
}
