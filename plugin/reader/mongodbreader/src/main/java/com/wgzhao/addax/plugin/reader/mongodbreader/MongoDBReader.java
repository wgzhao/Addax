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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.MongoUtil;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private boolean isNullOrEmpty(String obj)
        {
            return obj == null || obj.isEmpty();
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            String userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            String password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTH_DB, database);
            if (!isNullOrEmpty((userName)) && !isNullOrEmpty((password))) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig, userName, password, authDb);
            }
            else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Reader.Task
    {

        private MongoClient mongoClient;

        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        private boolean isNullOrEmpty(String obj)
        {
            return obj == null || obj.isEmpty();
        }

        @Override
        public void startRead(RecordSender recordSender)
        {

            if (lowerBound == null || upperBound == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumnMeta == null) {
                throw AddaxException.asAddaxException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor;
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }
            }
            else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            }
            else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound)
                        .append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }
            if (!isNullOrEmpty((query))) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }
            dbCursor = col.find(filter).iterator();
            Document item;
            if (mongodbColumnMeta.size() == 1 && mongodbColumnMeta.get(0) == "*") {
                item = dbCursor.next();
                mongodbColumnMeta = JSON.parseArray(item.toJson());
            }
            while (dbCursor.hasNext()) {
                item = dbCursor.next();
                Record record = recordSender.createRecord();

                for (Object o : mongodbColumnMeta) {
                    JSONObject column = (JSONObject) o;
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    if (tempCol == null && KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
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

                            tempCol = nestedDocument.get(name[name.length - 1]);
                        }
                    }
                    if (tempCol == null) {
                        //continue; 这个不能直接continue会导致record到目的端错位
                        record.addColumn(new StringColumn());
                    }
                    else if (tempCol instanceof Double) {
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    }
                    else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    }
                    else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    }
                    else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    }
                    else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    }
                    else if (tempCol instanceof Document) {
                        if (KeyConstant.isJsonType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            record.addColumn(new StringColumn(((Document) tempCol).toJson()));
                        }
                        else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                    else {
                        if (KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if (isNullOrEmpty((splitter))) {
                                throw AddaxException.asAddaxException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                            }
                            else {
                                ArrayList<String> array = (ArrayList) tempCol;
                                //String tempArrayStr = Joiner.on(splitter).join(array)
                                String tempArrayStr = String.join(splitter, array);
                                record.addColumn(new StringColumn(tempArrayStr));
                            }
                        }
                        else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init()
        {
            Configuration readerSliceConfig = getPluginJobConf();
            String userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            String password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTH_DB, this.database);
            if (!isNullOrEmpty((userName)) && !isNullOrEmpty((password))) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName, password, authDb);
            }
            else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            List<String> column = readerSliceConfig.getList(KeyConstant.MONGO_COLUMN, String.class);
            if (column.size() == 1 && "*".equals(column.get(0))) {
                // TODO get all columns from record
            }
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECT_ID);
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
