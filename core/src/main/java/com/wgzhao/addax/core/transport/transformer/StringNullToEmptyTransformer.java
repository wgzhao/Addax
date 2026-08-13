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

import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;

/**
 * Transformer that converts null string values to empty strings.
 *
 * Processes ALL columns automatically - no need to specify columnIndex.
 * Only transforms columns that are StringColumn type with null values.
 *
 * Usage in job config:
 * "transformer": [
 *   {
 *     "name": "dx_string_null_to_empty"
 *   }
 * ]
 */
public class StringNullToEmptyTransformer
        extends Transformer
{
    public StringNullToEmptyTransformer()
    {
        setTransformerName("dx_string_null_to_empty");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {
        if (record == null) {
            return null;
        }

        // Process all columns
        for (int i = 0; i < record.getColumnNumber(); i++) {
            if (record.getColumn(i) == null || record.getColumn(i).getRawData() == null) {
                // Only convert to empty string if it's a StringColumn
                if (record.getColumn(i) instanceof StringColumn) {
                    record.setColumn(i, new StringColumn(""));
                }
                // For null column (no type info), also convert to empty string
                else if (record.getColumn(i) == null) {
                    record.setColumn(i, new StringColumn(""));
                }
            }
        }

        return record;
    }
}
