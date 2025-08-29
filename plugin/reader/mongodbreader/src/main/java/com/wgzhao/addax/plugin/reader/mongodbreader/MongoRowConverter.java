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

import com.mongodb.client.MongoDatabase;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.mongodb.client.MongoCollection;
import com.wgzhao.addax.core.element.StringColumn;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * MongoDB row converter that transforms BSON documents into Addax records.
 * This class handles the conversion of MongoDB BSON data types to corresponding
 * Addax column types with caching mechanism for performance optimization.
 *
 * @author wgzhao
 * @since 6.0.5
 */
public class MongoRowConverter
{
    /**
     * Cache for column handlers to improve performance by avoiding repeated
     * handler creation for the same column types.
     */
    private final Map<String, BiConsumer<BsonValue, Record>> handlers = new ConcurrentHashMap<>();

    /**
     * Processes a single BSON document and converts specified columns to record format.
     * This method iterates through the provided columns and converts each BSON value
     * to the appropriate Addax column type.
     *
     * @param doc the BSON document to process
     * @param record the target record to populate with converted columns
     * @param columns an iterable of column names to extract from the document
     */
    public void processOne(BsonDocument doc, Record record, Iterable<String> columns)
    {
        for (String col : columns) {
            if (col.startsWith("'")) {
                record.addColumn(new StringColumn(col.substring(1, col.length() - 1)));
                continue;
            }
            BsonValue val = doc.get(col);
            if (val == null || val.isNull()) {
                record.addColumn(new StringColumn());
                continue;
            }
            BiConsumer<BsonValue, Record> h = handlers.get(col);
            if (h != null) {
                h.accept(val, record);
                continue;
            }
            BiConsumer<BsonValue, Record> generated = createHandler(val.getBsonType());
            handlers.put(col, generated);
            generated.accept(val, record);
        }
    }

    /**
     * Creates a handler function for converting BSON values to Addax columns
     * based on the BSON type. This method uses a switch expression to map
     * each BSON type to its corresponding Addax column type.
     *
     * @param t the BSON type to create a handler for
     * @return a BiConsumer that can convert BSON values of the specified type
     *         to appropriate Addax columns
     */
    private BiConsumer<BsonValue, Record> createHandler(BsonType t)
    {
        return switch (t) {
            case DOUBLE -> (v, r) -> r.addColumn(new DoubleColumn(v.asDouble().getValue()));
            case INT32 -> (v, r) -> r.addColumn(new LongColumn(v.asInt32().getValue()));
            case INT64 -> (v, r) -> r.addColumn(new LongColumn(v.asInt64().getValue()));
            case DECIMAL128 -> (v, r) -> r.addColumn(new DoubleColumn(v.asDecimal128().getValue().toString()));
            case BOOLEAN -> (v, r) -> r.addColumn(new BoolColumn(v.asBoolean().getValue()));
            case DATE_TIME -> (v, r) -> r.addColumn(new DateColumn(new java.util.Date(v.asDateTime().getValue())));
            case DOCUMENT -> (v, r) -> r.addColumn(new StringColumn(v.asDocument().toJson()));
            case OBJECT_ID -> (v, r) -> r.addColumn(new StringColumn(v.asObjectId().getValue().toHexString()));
            case STRING -> (v, r) -> r.addColumn(new StringColumn(v.asString().getValue()));
            case ARRAY -> (v, r) -> r.addColumn(new StringColumn((v.asArray().getValues().toString())));
            default -> (v, r) -> r.addColumn(new StringColumn(v.toString()));
        };
    }

    /**
     * Retrieves a MongoDB collection configured to work with BSON documents.
     * This method returns a collection that can be used to query MongoDB
     * and receive results as BSON documents.
     *
     * @param db the MongoDB database instance
     * @param name the name of the collection to retrieve
     * @return a MongoCollection configured for BSON document operations
     */
    public MongoCollection<BsonDocument> getBsonCollection(MongoDatabase db, String name)
    {
        return db.getCollection(name, BsonDocument.class);
    }
}
