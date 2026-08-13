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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;

import java.util.HashSet;
import java.util.Set;

/**
 * Transformer that converts null values to empty strings for string-type columns.
 *
 * Requires column type mapping from writer config to identify string columns.
 *
 * Usage in job config:
 * "transformer": [
 *   {
 *     "name": "dx_string_null_to_empty",
 *     "parameter": {
 *       "column": [
 *         {"name": "euc", "type": "string"},
 *         {"name": "eu1", "type": "string"},
 *         {"name": "age", "type": "long"}
 *       ]
 *     }
 *   }
 * ]
 *
 * Only columns with type "string" will have null converted to "".
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

        // Parse column config from parameters
        Set<Integer> stringColumnIndexes = parseStringColumnIndexes(paras);

        if (stringColumnIndexes == null) {
            // No column config provided, skip transformation
            return record;
        }

        // Only transform string columns
        for (int i = 0; i < record.getColumnNumber(); i++) {
            if (stringColumnIndexes.contains(i)) {
                if (record.getColumn(i) == null || record.getColumn(i).getRawData() == null) {
                    record.setColumn(i, new StringColumn(""));
                }
            }
        }

        return record;
    }

    @SuppressWarnings("unchecked")
    private Set<Integer> parseStringColumnIndexes(Object... paras)
    {
        if (paras == null || paras.length == 0) {
            return null;
        }

        try {
            // paras[0] should be the parameter map from transformer config
            if (paras[0] instanceof String) {
                // If it's a JSON string, parse it
                JSONObject param = JSON.parseObject((String) paras[0]);
                return extractStringColumns(param);
            }
            else if (paras[0] instanceof JSONObject) {
                return extractStringColumns((JSONObject) paras[0]);
            }
        }
        catch (Exception e) {
            // If parsing fails, return null
        }

        return null;
    }

    private Set<Integer> extractStringColumns(JSONObject param)
    {
        Set<Integer> stringIndexes = new HashSet<>();
        JSONArray columns = param.getJSONArray("column");

        if (columns == null) {
            return null;
        }

        for (int i = 0; i < columns.size(); i++) {
            JSONObject col = columns.getJSONObject(i);
            String type = col.getString("type");
            if ("string".equalsIgnoreCase(type) || "str".equalsIgnoreCase(type)) {
                stringIndexes.add(i);
            }
        }

        return stringIndexes;
    }
}
