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

import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * SubstrTransformer extracts a substring from a designated column value.
 */
public class SubstrTransformer
        extends Transformer
{
    private int columnIndex;
    private int startIndex;
    private int length;

    public SubstrTransformer()
    {
        setTransformerName("dx_substr");
    }

    @Override
    protected void doInit(Object[] paras)
    {
        if (paras == null || paras.length != 3) {
            throw configError("requires 3 parameters: idx, pos and length");
        }
        columnIndex = intParam(paras[0], "idx");
        startIndex = intParam(paras[1], "pos");
        length = intParam(paras[2], "length");
        if (columnIndex < 0) {
            throw configError("the parameter [idx] must not be negative");
        }
        if (startIndex < 0) {
            throw configError("the parameter [pos] must not be negative");
        }
        if (length < 0) {
            throw configError("the parameter [length] must not be negative");
        }
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
        init(paras);

        try {
            Column column = record.getColumn(columnIndex);
            if (column == null) {
                throw new RuntimeException(String.format("columnIndex[%d] out of bound[%d]",
                        columnIndex, record.getColumnNumber()));
            }
            String oriValue = column.asString();
            // If the field is null, skip substring processing
            if (oriValue == null) {
                return record;
            }

            if (startIndex > oriValue.length()) {
                throw new RuntimeException(String.format("The dx_substr startIndex(%s) out of range" +
                        "(%s) of (%s)", startIndex, oriValue.length(), oriValue));
            }

            long endIndex = (long) startIndex + length;
            String newValue = endIndex >= oriValue.length()
                    ? oriValue.substring(startIndex)
                    : oriValue.substring(startIndex, (int) endIndex);

            record.setColumn(columnIndex, new StringColumn(newValue));
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e.getMessage(), e);
        }
        return record;
    }
}
