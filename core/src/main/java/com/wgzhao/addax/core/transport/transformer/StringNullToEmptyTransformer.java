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
 * Operates on a single column specified by columnIndex.
 * Only transforms if the column type parameter is "string".
 *
 * Usage in job config:
 * "transformer": [
 *   {
 *     "name": "dx_string_null_to_empty",
 *     "parameter": {
 *       "columnIndex": 0,
 *       "paras": ["string"]
 *     }
 *   }
 * ]
 *
 * For multiple columns, add multiple transformer entries:
 * "transformer": [
 *   {"name": "dx_string_null_to_empty", "parameter": {"columnIndex": 0, "paras": ["string"]}},
 *   {"name": "dx_string_null_to_empty", "parameter": {"columnIndex": 1, "paras": ["string"]}}
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

        // paras[0] is columnIndex (set by framework)
        // paras[1] should be the column type (e.g., "string")
        if (paras == null || paras.length < 2) {
            return record;
        }

        int columnIndex = (Integer) paras[0];
        String columnType = (String) paras[1];

        // Only transform string type columns
        if (!"string".equalsIgnoreCase(columnType) && !"str".equalsIgnoreCase(columnType)) {
            return record;
        }

        // Check bounds
        if (columnIndex < 0 || columnIndex >= record.getColumnNumber()) {
            return record;
        }

        // Convert null to empty string
        if (record.getColumn(columnIndex) == null || record.getColumn(columnIndex).getRawData() == null) {
            record.setColumn(columnIndex, new StringColumn(""));
        }

        return record;
    }
}
