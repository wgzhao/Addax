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

import com.wgzhao.addax.common.element.Record;

import java.util.List;

public class DatabendCsvCodec
        extends DatabendCodec
{
    private final String columnSeparator;

    public DatabendCsvCodec(final List<String> fieldNames, String columnSeparator)
    {
        super(fieldNames);
        this.columnSeparator = DatabendDelimiterParser.parse(columnSeparator, "\t");
    }

    @Override
    public String serialize(Record row)
    {
        StringBuilder sb = new StringBuilder();
        int col_num = row.getColumnNumber();
        int add_col_sep = row.getColumnNumber() - 1;
        for (int i = 0; i < col_num; i++) {
            String value = fieldConvertion(row.getColumn(i));
            sb.append(null == value ? "\\N" : value);
            if (i < add_col_sep) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
