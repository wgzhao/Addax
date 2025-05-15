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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.EncryptUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.MongoUtil;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.DATABASE;
import static com.wgzhao.addax.core.base.Key.FETCH_SIZE;
import static com.wgzhao.addax.core.base.Key.PASSWORD;
import static com.wgzhao.addax.core.base.Key.USERNAME;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

public class MongoDBReader
        extends Reader
{

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private boolean notNullAndEmpty(String obj)
        {
            return obj != null && !obj.isEmpty();
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
            // check required configuration
            String userName = originalConfig.getNecessaryValue(USERNAME, REQUIRED_VALUE);
            String password = originalConfig.getString(PASSWORD);
            if (password != null && password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
                // encrypted password, need to decrypt
                password = EncryptUtil.decrypt(password.substring(6, password.length() - 1));
                originalConfig.set(Key.PASSWORD, password);
            }
            Configuration connConf = originalConfig.getConfiguration(CONNECTION);
            String database = connConf.getNecessaryValue(DATABASE, REQUIRED_VALUE);
            String authDb = connConf.getString(KeyConstant.MONGO_AUTH_DB, database);
            List<Object> addressList = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            List<String> columns = originalConfig.getList(COLUMN, String.class);
            if (columns == null || (columns.size() == 1 && "*".equals(columns.get(0)))) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The configuration column must be required and DOES NOT support \"*\" yet");
            }
            if (notNullAndEmpty((userName)) && notNullAndEmpty((password))) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(addressList, userName, password, authDb);
            }
            else {
                this.mongoClient = MongoUtil.initMongoClient(addressList);
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

        private List<String> mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;
        private int fetchSize;

        private record MongoQueryFilter(Document filter, boolean isObjectId) {}

        private boolean notNullAndEmpty(String obj)
        {
            return obj != null && !obj.isEmpty();
        }

        private MongoQueryFilter buildQueryFilter()
        {
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID,
                            new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }
            }
            else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID,
                        new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            }
            else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID,
                        new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound)
                                .append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }

            if (query != null && !query.isEmpty()) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", List.of(filter, queryFilter));
            }

            return new MongoQueryFilter(filter, isObjectId);
        }

        private void addColumnToRecord(Record record, Document item, String column)
        {
            if (column.startsWith("'")) {
                record.addColumn(new StringColumn(column.replace("'", "")));
                return;
            }

            try {
                Double numValue = Double.parseDouble(column);
                record.addColumn(column.contains(".") ?
                        new DoubleColumn(numValue) :
                        new LongColumn(Long.parseLong(column)));
                return;
            }
            catch (NumberFormatException ignored) {
                // Not a number, continue with normal processing
            }

            if (!item.containsKey(column)) {
                record.addColumn(new StringColumn());
                return;
            }

            Object tempCol = item.get(column);
            if (tempCol == null) {
                record.addColumn(new StringColumn());
                return;
            }

            if (tempCol instanceof Double d) {
                record.addColumn(new DoubleColumn(d));
            }
            else if (tempCol instanceof Boolean b) {
                record.addColumn(new BoolColumn(b));
            }
            else if (tempCol instanceof Date d) {
                record.addColumn(new DateColumn(d));
            }
            else if (tempCol instanceof Integer i) {
                record.addColumn(new LongColumn(i));
            }
            else if (tempCol instanceof Long l) {
                record.addColumn(new LongColumn(l));
            }
            else if (tempCol instanceof Document doc) {
                record.addColumn(new StringColumn(doc.toJson()));
            }
            else {
                record.addColumn(new StringColumn(tempCol.toString()));
            }
        }

        @Override
        public void startRead(RecordSender recordSender)
        {

            if (lowerBound == null || upperBound == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumnMeta == null) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        ILLEGAL_VALUE.getDescription());
            }

            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(this.collection);

            MongoQueryFilter queryFilter = buildQueryFilter();
            try (MongoCursor<Document> dbCursor = col.find(queryFilter.filter())
                    .batchSize(fetchSize)
                    .iterator()) {

                while (dbCursor.hasNext()) {
                    Document item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    mongodbColumnMeta.forEach(column -> addColumnToRecord(record, item, column));
                    recordSender.sendToWriter(record);
                }
            }
        }

        @Override
        public void init()
        {
            Configuration readerSliceConfig = getPluginJobConf();
            String userName = readerSliceConfig.getString(USERNAME);
            String password = readerSliceConfig.getString(PASSWORD);
            if (password != null && password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
                // encrypted password, need to decrypt
                password = EncryptUtil.decrypt(password.substring(6, password.length() - 1));
            }
            this.fetchSize = readerSliceConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = readerSliceConfig.getList(COLUMN, String.class);
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECT_ID);

            Configuration connConf = readerSliceConfig.getConfiguration(CONNECTION);
            this.database = connConf.getString(DATABASE);
            this.collection = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);
            String authDb = connConf.getString(KeyConstant.MONGO_AUTH_DB, this.database);
            List<Object> addressList = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            if (notNullAndEmpty((userName)) && notNullAndEmpty((password))) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(addressList, userName, password, authDb);
            }
            else {
                this.mongoClient = MongoUtil.initMongoClient(addressList);
            }
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
