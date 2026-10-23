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

import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;
import static com.wgzhao.addax.core.util.MathUtil.add;
import static com.wgzhao.addax.core.util.MathUtil.divide;
import static com.wgzhao.addax.core.util.MathUtil.mod;
import static com.wgzhao.addax.core.util.MathUtil.multiply;
import static com.wgzhao.addax.core.util.MathUtil.pow;
import static com.wgzhao.addax.core.util.MathUtil.subtract;

/**
 * Applies an arithmetic operation to the configured numeric column.
 */
public class MapTransformer
        extends Transformer
{
    private static final List<String> SUPPORTED_OPERATORS = List.of("+", "-", "*", "/", "%", "^");
    private static final int DEFAULT_SCALE = 2;

    private int columnIndex;
    private String operator;
    private String value;

    public MapTransformer()
    {
        setTransformerName("dx_map");
    }

    @Override
    protected void doInit(Object[] paras)
    {
        if (paras == null || paras.length != 3) {
            throw configError("requires 3 parameters: idx, operator and value");
        }
        columnIndex = intParam(paras[0], "idx");
        operator = stringParam(paras[1], "operator").trim();
        value = stringParam(paras[2], "value");

        if (columnIndex < 0) {
            throw configError("the parameter [idx] must not be negative");
        }
        if (!SUPPORTED_OPERATORS.contains(operator)) {
            throw configError("the operator [" + operator + "] is unsupported, supported operators are "
                    + SUPPORTED_OPERATORS);
        }
        try {
            Double.valueOf(value);
        }
        catch (NumberFormatException e) {
            throw configError("the parameter [value] must be a number, but was [" + value + "]");
        }
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
            if (column.getRawData() == null) {
                return record;
            }

            // the column value is data, not configuration: it still has to be checked
            // for every record before the arithmetic helper is called
            Double.valueOf(column.asString());

            int scale = DEFAULT_SCALE;
            String oriValue = column.asString();
            int dot = oriValue.indexOf('.');
            // a trailing dot has no fractional digits, matching String.split(".") semantics
            if (dot >= 0 && dot < oriValue.length() - 1) {
                scale = oriValue.length() - dot - 1;
            }

            String newValue = switch (operator) {
                case "+" -> add(oriValue, value);
                case "-" -> subtract(oriValue, value);
                case "*" -> multiply(oriValue, value);
                case "/" -> divide(oriValue, value, scale);
                case "%" -> mod(oriValue, value);
                case "^" -> pow(oriValue, value);
                default -> throw new RuntimeException("dx_map can't support code:" + operator);
            };
            record.setColumn(columnIndex, new StringColumn(newValue));
            return record;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    RUNTIME_ERROR, e.getMessage(), e);
        }
    }
}
