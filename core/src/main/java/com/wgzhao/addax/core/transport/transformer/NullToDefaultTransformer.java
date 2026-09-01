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
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transformer that converts null values to type-appropriate defaults.
 *
 * Reads the writer's column config from parameter to determine target types.
 * For null values, creates a default column of the target type.
 * Non-null values are passed through unchanged.
 *
 * Usage in job config:
 * "transformer": [
 *   {
 *     "name": "dx_null_to_default",
 *     "parameter": {
 *       "column": [
 *         {"name": "id", "type": "string"},
 *         {"name": "amount", "type": "double"},
 *         {"name": "count", "type": "long"}
 *       ]
 *     }
 *   }
 * ]
 */
public class NullToDefaultTransformer
        extends Transformer
{
    // the column config JSON is fixed per job; parse it once per distinct parameter string
    private final Map<String, String[]> columnTypesCache = new ConcurrentHashMap<>();

    public NullToDefaultTransformer()
    {
        setTransformerName("dx_null_to_default");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {
        if (record == null) {
            return null;
        }

        // Parse writer column config from parameter
        String[] columnTypes = parseColumnTypes(paras);
        if (columnTypes == null) {
            // No parameter, skip transformation
            return record;
        }

        // Process all columns
        for (int i = 0; i < record.getColumnNumber(); i++) {
            if (i < columnTypes.length && columnTypes[i] != null) {
                if (record.getColumn(i) == null || record.getColumn(i).getRawData() == null) {
                    // Convert null to default value based on writer type
                    record.setColumn(i, createDefaultColumn(columnTypes[i]));
                }
            }
        }

        return record;
    }

    private String[] parseColumnTypes(Object... paras)
    {
        if (paras == null || paras.length == 0) {
            return null;
        }

        try {
            // paras[0] should be the parameter JSON string
            String paramJson = null;
            if (paras[0] instanceof String) {
                paramJson = (String) paras[0];
            } else if (paras[0] instanceof JSONObject) {
                paramJson = ((JSONObject) paras[0]).toJSONString();
            }

            if (paramJson == null) {
                return null;
            }

            // the config is immutable per job, so parsing it once per distinct
            // parameter string avoids a JSON.parseObject on every record
            return columnTypesCache.computeIfAbsent(paramJson, this::parseColumnTypesFromJson);
        } catch (Exception e) {
            return null;
        }
    }

    private String[] parseColumnTypesFromJson(String paramJson)
    {
        JSONObject param = JSON.parseObject(paramJson);
        JSONArray columns = param.getJSONArray("column");
        if (columns == null || columns.isEmpty()) {
            return null;
        }

        String[] types = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            JSONObject col = columns.getJSONObject(i);
            types[i] = col.getString("type");
        }
        return types;
    }

    private Column createDefaultColumn(String type)
    {
        if (type == null) {
            return new StringColumn("");
        }

        switch (type.toLowerCase()) {
            case "string":
            case "char":
            case "varchar":
            case "text":
                return new StringColumn("");

            case "int":
            case "integer":
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "bigint":
            case "long":
                return new LongColumn(0L);

            case "float":
            case "double":
            case "decimal":
            case "numeric":
            case "real":
                return new DoubleColumn(0.0);

            case "bool":
            case "boolean":
                return new BoolColumn(false);

            case "date":
            case "datetime":
            case "timestamp":
            case "time":
                return new StringColumn("");

            case "bytes":
            case "binary":
            case "varbinary":
            case "blob":
                return new BytesColumn(new byte[0]);

            case "array":
            case "json":
            case "jsonb":
            case "object":
            case "objectid":
            case "java_object":
                return new StringColumn("");

            default:
                return new StringColumn("");
        }
    }
}
