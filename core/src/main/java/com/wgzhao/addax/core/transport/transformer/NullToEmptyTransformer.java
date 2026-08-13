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

/**
 * Transformer that converts null values to empty strings.
 *
 * Usage in job config:
 * "transformer": [
 *   {
 *     "name": "dx_null_to_empty",
 *     "parameter": {
 *       "columnIndex": 0  // optional: specific column index, or omit for all columns
 *     }
 *   }
 * ]
 */
public class NullToEmptyTransformer
        extends Transformer
{
    public NullToEmptyTransformer()
    {
        setTransformerName("dx_null_to_empty");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {
        if (record == null) {
            return null;
        }

        // Check if specific column index is provided
        if (paras != null && paras.length > 0) {
            // Transform specific column
            int columnIndex;
            try {
                columnIndex = (Integer) paras[0];
            }
            catch (Exception e) {
                // If parameter is not a valid index, transform all columns
                columnIndex = -1;
            }

            if (columnIndex >= 0 && columnIndex < record.getColumnNumber()) {
                replaceNullWithEmpty(record, columnIndex);
            }
        }
        else {
            // Transform all columns
            for (int i = 0; i < record.getColumnNumber(); i++) {
                replaceNullWithEmpty(record, i);
            }
        }

        return record;
    }

    private void replaceNullWithEmpty(Record record, int columnIndex)
    {
        Column column = record.getColumn(columnIndex);
        if (column == null || column.getRawData() == null) {
            record.setColumn(columnIndex, new StringColumn(""));
        }
    }
}
