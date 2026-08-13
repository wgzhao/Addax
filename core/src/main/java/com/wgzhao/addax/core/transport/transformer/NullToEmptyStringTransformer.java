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
 * Transformer that converts null string values to empty strings.
 *
 * Only transforms columns that are already StringColumn type.
 * Other types (Long, Double, Bool, etc.) are left unchanged.
 *
 * Usage in job config:
 * "transformer": [
 *   {
 *     "name": "dx_null_to_empty_string"
 *   }
 * ]
 */
public class NullToEmptyStringTransformer
        extends Transformer
{
    public NullToEmptyStringTransformer()
    {
        setTransformerName("dx_null_to_empty_string");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {
        if (record == null) {
            return null;
        }

        for (int i = 0; i < record.getColumnNumber(); i++) {
            Column column = record.getColumn(i);
            // 只处理 StringColumn 类型的 null 值
            if (column instanceof StringColumn && column.getRawData() == null) {
                record.setColumn(i, new StringColumn(""));
            }
        }

        return record;
    }
}
