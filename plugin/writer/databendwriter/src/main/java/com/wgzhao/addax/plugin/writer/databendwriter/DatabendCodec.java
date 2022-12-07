/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.writer.databendwriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.Record;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.List;
import java.util.TimeZone;

public abstract class DatabendCodec
{
    protected static String timeZone = "GMT+8";
    protected static TimeZone timeZoner = TimeZone.getTimeZone(timeZone);
    protected final List<String> fieldNames;

    public DatabendCodec(final List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public abstract String serialize(Record row);

    protected String fieldConvertion(Column col)
    {
        if (null == col.getRawData() || Column.Type.NULL == col.getType()) {
            return null;
        }
        Column.Type type = col.getType();
        switch (type) {
            case BOOL:
                return String.valueOf(col.asLong());
            case BYTES:
                byte[] bts = (byte[]) col.getRawData();
                long value = 0;
                for (int i = 0; i < bts.length; i++) {
                    value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
                }
                return String.valueOf(value);
            case STRING:
                String col_string = col.asString();
                if (col_string.contains("\"")) {
                    String col_rep = col_string.replaceAll("\"","\"\"");
                    return "\"" + col_rep + "\"";
                }
                return "\"" + col_string + "\"";
            case TIMESTAMP:
            case DATE:
                return "\"" + col.asString() + "\"";
            default:
                return col.asString();
        }
    }

    /**
     * convert datax internal  data to string
     *
     * @param col
     * @return
     */
    protected Object convertColumn(final Column col) {
        if (null == col.getRawData()) {
            return null;
        }
        Column.Type type = col.getType();
        switch (type) {
            case BOOL:
            case INT:
            case LONG:
                return col.asLong();
            case DOUBLE:
                return col.asDouble();
            case STRING:
                return col.asString();
            case DATE: {
                final DateColumn.DateType dateType = ((DateColumn) col).getSubType();
                switch (dateType) {
                    case DATE:
                        return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd", timeZoner);
                    case DATETIME:
                        return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd HH:mm:ss", timeZoner);
                    default:
                        return col.asString();
                }
            }
            default:
                // BAD, NULL, BYTES
                return null;
        }
    }
}
