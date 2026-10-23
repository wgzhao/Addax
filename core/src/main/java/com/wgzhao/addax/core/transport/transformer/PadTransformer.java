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
 * Pads or truncates the configured column to a fixed length.
 */
public class PadTransformer
        extends Transformer
{
    private int columnIndex;
    private boolean padLeft;
    private int length;
    private String padString;

    public PadTransformer()
    {
        setTransformerName("dx_pad");
    }

    @Override
    protected void doInit(Object[] paras)
    {
        if (paras == null || paras.length != 4) {
            throw configError("requires 4 parameters: idx, flag, length and chr");
        }
        columnIndex = intParam(paras[0], "idx");
        String padType = stringParam(paras[1], "flag");
        length = intParam(paras[2], "length");
        padString = stringParam(paras[3], "chr");

        if (columnIndex < 0) {
            throw configError("the parameter [idx] must not be negative");
        }
        if (length < 0) {
            throw configError("the parameter [length] must not be negative");
        }
        if (!"l".equalsIgnoreCase(padType) && !"r".equalsIgnoreCase(padType)) {
            throw configError("the parameter [flag] must be either l or r, but was [" + padType + "]");
        }
        if (padString.isEmpty()) {
            // an empty pad string would never reduce the remaining length and hang the job
            throw configError("the parameter [chr] must not be empty");
        }
        padLeft = "l".equalsIgnoreCase(padType);
    }

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

            if (oriValue == null) {
                oriValue = "";
            }
            String newValue;
            if (length <= oriValue.length()) {
                newValue = oriValue.substring(0, length);
            }
            else {
                newValue = doPad(oriValue);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e.getMessage(), e);
        }
        return record;
    }

    private String doPad(String oriValue)
    {
        StringBuilder finalPad = new StringBuilder();
        int needLength = length - oriValue.length();
        while (needLength > 0) {
            if (needLength >= padString.length()) {
                finalPad.append(padString);
                needLength -= padString.length();
            }
            else {
                finalPad.append(padString, 0, needLength);
                needLength = 0;
            }
        }

        return padLeft ? finalPad + oriValue : oriValue + finalPad;
    }
}
