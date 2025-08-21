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

package com.wgzhao.addax.core.transport.transformer;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;

import java.util.Arrays;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * SubstrTransformer extracts a substring from a designated column value.
 */
public class SubstrTransformer
        extends Transformer
{
    public SubstrTransformer()
    {
        setTransformerName("dx_substr");
    }

    /**
     * Evaluate and replace the specified column with a substring.
     *
     * @param record input record
     * @param paras parameters: [columnIndex:int, startIndex:String, length:String]
     * @return record with updated column, or original when input is null
     */
    @Override
    public Record evaluate(Record record, Object... paras)
    {

        int columnIndex;
        int startIndex;
        int length;

        try {
            if (paras.length != 3) {
                throw new RuntimeException("The dx_substr parameters must be 3");
            }

            columnIndex = (Integer) paras[0];
            startIndex = Integer.parseInt((String) paras[1]);
            length = Integer.parseInt((String) paras[2]);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();
            // If the field is null, skip substring processing
            if (oriValue == null) {
                return record;
            }

            if (startIndex > oriValue.length()) {
                throw new RuntimeException(String.format("The dx_substr startIndex(%s) out of range" +
                        "(%s) of (%s)", startIndex, oriValue.length(), oriValue));
            }

            String newValue;
            if (startIndex + length >= oriValue.length()) {
                newValue = oriValue.substring(startIndex);
            }
            else {
                newValue = oriValue.substring(startIndex, startIndex + length);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e.getMessage(), e);
        }
        return record;
    }
}
