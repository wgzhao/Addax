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
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.transformer.Transformer;

import java.util.Arrays;

import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.RUNTIME_ERROR;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class SubstrTransformer
        extends Transformer
{
    public SubstrTransformer()
    {
        setTransformerName("dx_substr");
    }

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
            //如果字段为空，跳过subStr处理
            if (oriValue == null) {
                return record;
            }
            String newValue;
            if (startIndex > oriValue.length()) {
                throw new RuntimeException(String.format("The dx_substr startIndex(%s) out of range" +
                        "(%s) of (%s)", startIndex, oriValue.length(), oriValue));
            }
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
