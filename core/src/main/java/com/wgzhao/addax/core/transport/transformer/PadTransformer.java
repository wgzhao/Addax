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

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.DataXException;
import com.wgzhao.addax.transformer.Transformer;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class PadTransformer
        extends Transformer
{
    public PadTransformer()
    {
        setTransformerName("dx_pad");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {

        int columnIndex;
        String padType;
        int length;
        String padString;

        try {
            if (paras.length != 4) {
                throw new RuntimeException("dx_pad paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            padType = (String) paras[1];
            length = Integer.parseInt((String) paras[2]);
            padString = (String) paras[3];
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                    "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            //如果字段为空，作为空字符串处理
            if (oriValue == null) {
                oriValue = "";
            }
            String newValue;
            if (!"r".equalsIgnoreCase(padType) && !"l".equalsIgnoreCase(padType)) {
                throw new RuntimeException(String.format("dx_pad first para(%s) support l or r", padType));
            }
            if (length <= oriValue.length()) {
                newValue = oriValue.substring(0, length);
            }
            else {

                newValue = doPad(padType, oriValue, length, padString);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }

    private String doPad(String padType, String oriValue, int length, String padString)
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

        if ("l".equalsIgnoreCase(padType)) {
            return finalPad + oriValue;
        }
        else {
            return oriValue + finalPad;
        }
    }
}
