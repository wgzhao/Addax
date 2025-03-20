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
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class ReplaceTransformer
        extends Transformer
{
    public ReplaceTransformer()
    {
        setTransformerName("dx_replace");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {

        int columnIndex;
        int startIndex;
        int length;
        String replaceString;
        try {
            if (paras.length != 4) {
                throw new RuntimeException("The dx_replace parameters must be 4");
            }

            columnIndex = (Integer) paras[0];
            startIndex = Integer.parseInt((String) paras[1]);
            length = Integer.parseInt((String) paras[2]);
            replaceString = (String) paras[3];
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            //如果字段为空，跳过replace处理
            if (oriValue == null) {
                return record;
            }
            String newValue;
            if (startIndex > oriValue.length()) {
                throw new RuntimeException(String.format("The dx_replace startIndex(%s) out of range(%s)",
                        startIndex, oriValue.length()));
            }
            if (startIndex + length >= oriValue.length()) {
                newValue = oriValue.substring(0, startIndex) + replaceString;
            }
            else {
                newValue = oriValue.substring(0, startIndex) + replaceString
                        + oriValue.substring(startIndex + length);
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
