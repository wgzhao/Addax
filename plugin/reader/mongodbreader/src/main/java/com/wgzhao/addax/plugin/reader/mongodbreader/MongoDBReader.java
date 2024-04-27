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

package com.wgzhao.addax.plugin.reader.mongodbreader;

import java.util.*;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.MongoUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.Range;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBReader extends Reader {
    private static final Logger LOG = LoggerFactory
        .getLogger(Reader.class);


    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;
        private String query = null;
        private String collection = null;

        @Override
        public List<Configuration> split(int adviceNumber) {

            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, this.mongoClient, query);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.collection = originalConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            this.query = originalConfig.getString(KeyConstant.MONGO_QUERY);
            if (!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig, userName, password, authDb);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private MongoClient mongoClient;
        private String userName = null;
        private String password = null;
        private String authDb = null;
        private String database = null;
        private String collection = null;
        private String query = null;
        private JSONArray mongodbColumnMeta = null;
        private String range = null;
        private boolean isObjectId = true;
        private int batchSize = 1000;
        private boolean jsonType = false;

        @Override
        public void startRead(RecordSender recordSender) {
            Range range = JSONObject.parseObject(this.range, Range.class);
            LOG.info("切片为:{}", this.range);
            MongoCursor<Document> dbCursor;
            if (range.isSampleType()) {
                dbCursor = queryByBound(range.getLowerBound(), range.getUpperBound());

            } else {
                dbCursor = queryBySkip(range.getSkip(), range.getLimit());

            }

            JsonWriterSettings settings = CollectionSplitUtil.getJsonWriterSettings();
            while (dbCursor.hasNext()) {
                Document item = dbCursor.next();
                // format json
                item = Document.parse(item.toJson(settings));

                Record record = recordSender.createRecord();

                if (jsonType) {
                    record.addColumn(new StringColumn(item.toJson()));
                    item = null;
                    recordSender.sendToWriter(record);
                    continue;
                }

                Iterator columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject) columnItera.next();
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    if (tempCol == null) {
                        if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String[] name = column.getString(KeyConstant.COLUMN_NAME).split("\\.");
                            if (name.length > 1) {
                                Object obj;
                                Document nestedDocument = item;
                                for (String str : name) {
                                    obj = nestedDocument.get(str);
                                    if (obj instanceof Document) {
                                        nestedDocument = (Document) obj;
                                    }
                                }

                                if (null != nestedDocument) {
                                    Document doc = nestedDocument;
                                    tempCol = doc.get(name[name.length - 1]);
                                }
                            }
                        }
                    }
                    if (tempCol == null) {
                        record.addColumn(new StringColumn(""));
                    } else if (tempCol instanceof Double) {
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    } else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    } else if (tempCol instanceof ArrayList && "STRING".equalsIgnoreCase(column.getString(KeyConstant.COLUMN_TYPE))) {
                        ArrayList array = (ArrayList) tempCol;
                        ArrayList resultList = new ArrayList<String>(array.size());
                        for (Object obj : array) {
                            if (obj instanceof Document) {
                                Document doc = (Document) obj;
                                resultList.add(doc.toJson());
                            } else {
                                resultList.add(obj);
                            }
                        }
                        record.addColumn(new StringColumn(JSON.toJSONString(resultList)));
                    } else if (tempCol instanceof Document && "STRING".equalsIgnoreCase(column.getString(KeyConstant.COLUMN_TYPE))) {
                        Document doc = (Document) tempCol;
                        record.addColumn(new StringColumn(doc.toJson()));
                    } else {
                        if (KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if (Strings.isNullOrEmpty(splitter)) {
                                throw AddaxException.asAddaxException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                            } else {
                                ArrayList array = (ArrayList) tempCol;
                                String tempArrayStr = Joiner.on(splitter).join(array);
                                record.addColumn(new StringColumn(tempArrayStr));
                            }
                        } else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        private MongoCursor<Document> queryBySkip(int skip, int limit) {
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(this.collection);

            Document filter = new Document();
            if (!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(queryFilter));
            }
            LOG.info("Filter by: " + filter.toJson());

            FindIterable<Document> findIterable = col.find(filter);
            return findIterable.skip(skip).limit(limit).batchSize(batchSize).iterator();
        }

        private MongoCursor<Document> queryByBound(String lowerBound, String upperBound) {
            if (lowerBound == null || upperBound == null ||
                mongoClient == null || database == null ||
                collection == null || mongodbColumnMeta == null) {
                throw AddaxException.asAddaxException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }

            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound) : upperBound));
                }
            } else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound) : lowerBound));
            } else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound) : upperBound));
            }
            if (!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }
            LOG.info("filter by ：" + filter.toJson());
            dbCursor = col.find(filter).batchSize(batchSize).iterator();
            return dbCursor;
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName, password, authDb);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.batchSize = readerSliceConfig.getInt(KeyConstant.BATCH_SIZE, 1000);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.range = readerSliceConfig.getString(KeyConstant.RANGE);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
            // 是否保存数据格式为json类型
            this.jsonType = readerSliceConfig.getBool(KeyConstant.JSON_TYPE, false);
            if (this.jsonType) {
                LOG.info("jsonType:{},导出数据为JSON类型，将会忽略用户配置的column", this.jsonType);
            }
        }

        @Override
        public void destroy() {

        }
    }
}
