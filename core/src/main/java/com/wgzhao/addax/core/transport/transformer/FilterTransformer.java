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

import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Filters rows by comparing one column with a configured value.
 *
 * <p>Returning {@code null} drops the record; returning the record keeps it. The
 * comparison value and the regular expression are parsed once when the transformer
 * is built, not once per record.
 */
public class FilterTransformer
        extends Transformer
{
    private static final List<String> SUPPORTED_OPERATORS = List.of(
            "like", "not like", ">", "<", "=", "==", "!=", ">=", "<=");

    private int columnIndex;
    private String operator;
    private String value;
    private Pattern likePattern;
    private Long longValue;
    private Double doubleValue;

    public FilterTransformer()
    {
        setTransformerName("dx_filter");
    }

    @Override
    protected void doInit(Object[] paras)
    {
        if (paras == null || paras.length != 3) {
            throw configError("requires 3 parameters: idx, operator and expr");
        }
        columnIndex = intParam(paras[0], "idx");
        operator = stringParam(paras[1], "operator").trim().toLowerCase(Locale.ROOT);
        value = stringParam(paras[2], "expr");

        if (columnIndex < 0) {
            throw configError("the parameter [idx] must not be negative");
        }
        if (value.isEmpty()) {
            throw configError("the parameter [expr] must not be empty");
        }
        if (!SUPPORTED_OPERATORS.contains(operator)) {
            throw configError("the operator [" + operator + "] is unsupported, supported operators are "
                    + SUPPORTED_OPERATORS);
        }

        if ("like".equals(operator) || "not like".equals(operator)) {
            try {
                likePattern = Pattern.compile(value);
            }
            catch (PatternSyntaxException e) {
                throw configError("the parameter [expr] is not a valid regular expression: " + e.getMessage());
            }
        }
        else {
            // numeric comparisons need a numeric value; a string column can still use
            // an arbitrary text, so a parse failure is not a configuration error here
            try {
                longValue = Long.valueOf(value);
            }
            catch (NumberFormatException ignored) {
                // the value may still be used against a string column
            }
            try {
                doubleValue = Double.valueOf(value);
            }
            catch (NumberFormatException ignored) {
                // the value may still be used against a string column
            }
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
            return switch (operator) {
                case "like" -> doLike(record, column);
                case "not like" -> doNotLike(record, column);
                case ">" -> doGreat(record, column, false);
                case "<" -> doLess(record, column, false);
                case "=", "==" -> doEqual(record, column);
                case "!=" -> doNotEqual(record, column);
                case ">=" -> doGreat(record, column, true);
                case "<=" -> doLess(record, column, true);
                default -> throw new RuntimeException("dx_filter code:" + operator + " is unsupported");
            };
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e.getMessage(), e);
        }
    }

    private Record doGreat(Record record, Column column, boolean hasEqual)
    {
        // If the field is null, skip comparison. Null is treated as negative infinity
        if (column.getRawData() == null) {
            return record;
        }
        if (column instanceof DoubleColumn) {
            double ori = column.asDouble();
            double val = requireDoubleValue();
            return hasEqual ? (ori >= val ? null : record) : (ori > val ? null : record);
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            long ori = column.asLong();
            long val = requireLongValue();
            return hasEqual ? (ori >= val ? null : record) : (ori > val ? null : record);
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            String ori = column.asString();
            return hasEqual ? (ori.compareTo(value) >= 0 ? null : record)
                    : (ori.compareTo(value) > 0 ? null : record);
        }
        else {
            throw new RuntimeException(">=,> can't support this columnType:" + column.getClass().getSimpleName());
        }
    }

    private Record doLess(Record record, Column column, boolean hasEqual)
    {
        // If the field is null, skip comparison. Null is treated as positive infinity
        if (column.getRawData() == null) {
            return record;
        }
        if (column instanceof DoubleColumn) {
            double ori = column.asDouble();
            double val = requireDoubleValue();
            return hasEqual ? (ori <= val ? null : record) : (ori < val ? null : record);
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            long ori = column.asLong();
            long val = requireLongValue();
            return hasEqual ? (ori <= val ? null : record) : (ori < val ? null : record);
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            String ori = column.asString();
            return hasEqual ? (ori.compareTo(value) <= 0 ? null : record)
                    : (ori.compareTo(value) < 0 ? null : record);
        }
        else {
            throw new RuntimeException("<=,< can't support this columnType:" + column.getClass().getSimpleName());
        }
    }

    /**
     * For DateColumn, compares the long value; for StringColumn, ByteColumn and BooleanColumn, compares their String values.
     *
     * @param record message record
     * @param column the column of record
     * @return Record (null to filter out the record, or non-null to keep)
     */
    private Record doEqual(Record record, Column column)
    {
        // If the field is null, only keep when the target value is "null"; otherwise do not filter nulls
        if (column.getRawData() == null) {
            return "null".equalsIgnoreCase(value) ? null : record;
        }

        if (column instanceof DoubleColumn) {
            return Double.compare(column.asDouble(), requireDoubleValue()) == 0 ? null : record;
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            return column.asLong() == requireLongValue() ? null : record;
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            return column.asString().compareTo(value) == 0 ? null : record;
        }
        else {
            throw new RuntimeException("== can't support this columnType:" + column.getClass().getSimpleName());
        }
    }

    /**
     * For DateColumn, it will compare long value; for StringColumn, ByteColumn and BooleanColumn, it will compare their String value.
     * @param record message record
     * @param column the column of record
     * @return Record (null to filter out the record, or non-null to keep)
     */
    private Record doNotEqual(Record record, Column column)
    {
        // If the field is null, only keep when the target value is "null", otherwise filter nulls
        if (column.getRawData() == null) {
            return "null".equalsIgnoreCase(value) ? record : null;
        }

        if (column instanceof DoubleColumn) {
            return Double.compare(column.asDouble(), requireDoubleValue()) != 0 ? null : record;
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            return column.asLong() != requireLongValue() ? null : record;
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            return column.asString().compareTo(value) != 0 ? null : record;
        }
        else {
            throw new RuntimeException("== can't support this columnType:" + column.getClass().getSimpleName());
        }
    }

    private Record doLike(Record record, Column column)
    {
        String originalValue = column.asString();
        return originalValue != null && likePattern.matcher(originalValue).matches() ? null : record;
    }

    private Record doNotLike(Record record, Column column)
    {
        String originalValue = column.asString();
        return originalValue != null && likePattern.matcher(originalValue).matches() ? record : null;
    }

    private long requireLongValue()
    {
        if (longValue == null) {
            throw new RuntimeException("The dx_filter expr [" + value + "] cannot be parsed as a long value");
        }
        return longValue;
    }

    private double requireDoubleValue()
    {
        if (doubleValue == null) {
            throw new RuntimeException("The dx_filter expr [" + value + "] cannot be parsed as a double value");
        }
        return doubleValue;
    }
}
