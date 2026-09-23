/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.reader.mongodbreader;

import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * MongoDB row converter that transforms BSON documents into Addax records.
 * Every configured column is resolved into a plan once per task, so the per record path
 * performs neither name parsing nor a type lookup through a cache.
 */
public class MongoRowConverter
{
    /**
     * A column name made of digits and at most one dot is a numeric constant, as documented.
     */
    private static final Pattern NUMBER_CONSTANT = Pattern.compile("\\d+(\\.\\d+)?");

    private final List<ColumnPlan> plans;

    /** The field paths to read, see {@link #projection()}. */
    private final List<String> projectionPaths;

    private final boolean wildcard;

    public MongoRowConverter(List<String> columns)
    {
        List<ColumnPlan> resolved = new ArrayList<>(columns.size());
        Set<String> fields = new LinkedHashSet<>();
        for (String column : columns) {
            ColumnPlan plan = ColumnPlan.of(column);
            resolved.add(plan);
            if (!plan.isConstant()) {
                fields.add(plan.name());
            }
        }
        this.plans = List.copyOf(resolved);
        this.wildcard = this.plans.size() == 1 && "*".equals(this.plans.get(0).name());

        List<String> paths = new ArrayList<>(fields.size());
        for (String field : fields) {
            // the server rejects a projection of a field together with a field below it, and
            // reading the wider one already covers the narrower, whatever order they came in
            if (fields.stream().noneMatch(other -> field.startsWith(other + "."))) {
                paths.add(field);
            }
        }
        this.projectionPaths = List.copyOf(paths);
    }

    /**
     * @return the fields this converter reads, or null to read the whole document
     */
    public Bson projection()
    {
        if (wildcard) {
            return null;
        }
        BsonDocument projection = new BsonDocument();
        for (String path : projectionPaths) {
            projection.put(path, new BsonInt32(1));
        }
        if (projection.isEmpty()) {
            // an empty projection would return every field, name one to keep the transfer minimal
            projection.put(KeyConstant.MONGO_PRIMARY_ID, new BsonInt32(1));
            return projection;
        }
        if (projectionPaths.stream().noneMatch(path -> path.equals(KeyConstant.MONGO_PRIMARY_ID)
                || path.startsWith(KeyConstant.MONGO_PRIMARY_ID + '.'))) {
            // an inclusion projection carries _id along, drop it when it is not read
            projection.put(KeyConstant.MONGO_PRIMARY_ID, new BsonInt32(0));
        }
        return projection;
    }

    /**
     * Convert one BSON document into a record.
     *
     * @param document the document read from the collection
     * @param record the target record to populate with converted columns
     */
    public void processOne(BsonDocument document, Record record)
    {
        if (wildcard) {
            // the whole document travels as a single JSON column
            record.addColumn(new StringColumn(document.toJson()));
            return;
        }
        for (ColumnPlan plan : plans) {
            record.addColumn(plan.toColumn(document));
        }
    }

    private static Column toColumn(BsonValue value)
    {
        return switch (value.getBsonType()) {
            case DOUBLE -> new DoubleColumn(value.asDouble().getValue());
            case INT32 -> new LongColumn(value.asInt32().getValue());
            case INT64 -> new LongColumn(value.asInt64().getValue());
            // a decimal keeps its exact textual form, a double would round it
            case DECIMAL128 -> new StringColumn(value.asDecimal128().getValue().toString());
            case BOOLEAN -> new BoolColumn(value.asBoolean().getValue());
            case DATE_TIME -> new DateColumn(new Date(value.asDateTime().getValue()));
            // a document is read as its extended JSON, the same text the wildcard column yields
            case DOCUMENT -> new StringColumn(value.asDocument().toJson());
            case OBJECT_ID -> new StringColumn(value.asObjectId().getValue().toHexString());
            case STRING -> new StringColumn(value.asString().getValue());
            case BINARY -> new BytesColumn(value.asBinary().getData());
            case ARRAY -> new StringColumn(toJson(value));
            // a timestamp, a regular expression or a minkey has no addax counterpart,
            // its extended JSON keeps the value readable and parseable
            default -> new StringColumn(toJson(value));
        };
    }

    /**
     * Serialize a BSON value to extended JSON. Only a document can do that directly, so a value
     * of another type is wrapped and the wrapper stripped again. This keeps arrays readable as
     * valid JSON instead of a Java list representation.
     */
    private static String toJson(BsonValue value)
    {
        if (value instanceof BsonDocument document) {
            return document.toJson();
        }
        String json = new BsonDocument("v", value).toJson();
        return json.substring(json.indexOf(':') + 1, json.length() - 1).trim();
    }

    /**
     * A configured column resolved once: either a constant appended to every record, or a field
     * of the document addressed by a pre-split dotted path.
     */
    private record ColumnPlan(String name, String[] path, Column constant)
    {
        static ColumnPlan of(String column)
        {
            // a quoted column is the documented way to append a constant string,
            // and a numeric column is appended as a number
            if (column.length() >= 2 && column.startsWith("'") && column.endsWith("'")) {
                return new ColumnPlan(column, null, new StringColumn(column.substring(1, column.length() - 1)));
            }
            if (NUMBER_CONSTANT.matcher(column).matches()) {
                return new ColumnPlan(column, null, column.contains(".")
                        ? new DoubleColumn(Double.parseDouble(column))
                        : new LongColumn(Long.parseLong(column)));
            }
            return new ColumnPlan(column, column.contains(".") ? column.split("\\.") : new String[] {column}, null);
        }

        boolean isConstant()
        {
            return constant != null;
        }

        Column toColumn(BsonDocument document)
        {
            if (constant != null) {
                // a column is never written to, one instance can back every record
                return constant;
            }
            BsonValue value = get(document);
            return value == null ? new StringColumn() : MongoRowConverter.toColumn(value);
        }

        /**
         * @return the value at the dotted path, null if a part of the path is absent
         */
        private BsonValue get(BsonDocument document)
        {
            BsonValue current = document;
            for (String key : path) {
                if (!current.isDocument()) {
                    return null;
                }
                // an absent key reads as null, a key holding a bson null reads as BsonNull
                current = current.asDocument().get(key);
                if (current == null) {
                    return null;
                }
            }
            // an absent field and a null value are both read as an empty column
            return current.isNull() ? null : current;
        }
    }
}
