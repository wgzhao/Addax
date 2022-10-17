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

package com.wgzhao.addax.plugin.reader.kafkareader;

import com.wgzhao.addax.common.spi.ErrorCode;

public enum KafkaReaderErrorCode
        implements ErrorCode
{
    REQUIRED_VALUE("KafkaReader-00", "You are missing a required parameter value."),
    ILLEGAL_VALUE("KafkaReader-01", "You fill in the parameter values are not legitimate."),
    GET_KAFKA_CONNECTION_ERROR("KafkaReader-02", "Error getting KAFKA connection."),
    GET_KAFKA_TABLE_ERROR("KafkaReader-03", "Error getting KAFKA table."),
    CLOSE_KAFKA_CONNECTION_ERROR("KafkaReader-04", "Error closing KAFKA connection."),
    CLOSE_KAFKA_SESSION_ERROR("KafkaReader-06", "Error closing KAFKA table connection."),
    PUT_KAFKA_ERROR("KafkaReader-07", "IO exception occurred when writing to KAFKA."),
    DELETE_KAFKA_ERROR("KafkaReader-08", "An exception occurred while delete KAFKA table."),
    CREATE_KAFKA_TABLE_ERROR("KafkaReader-09", "Error creating KAFKA table."),
    PARAMETER_NUM_ERROR("KafkaReader-10", "The number of parameters does not match."),
    TABLE_NOT_EXISTS("KafkaReader-11", "The table you specified does not exists yet"),
    COLUMN_NOT_EXISTS("KafkaReader-12", "the column doest not exists"),
    NOT_MATCHED_COLUMNS("KafkaReader-13", "the number of columns does not match the record");

    private final String code;
    private final String description;

    KafkaReaderErrorCode(String code, String description)
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
