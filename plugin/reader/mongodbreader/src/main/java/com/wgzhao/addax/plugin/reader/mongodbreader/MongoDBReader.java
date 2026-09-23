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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.EncryptUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.wgzhao.addax.plugin.reader.mongodbreader.util.MongoUtil;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
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

/** Mongo DBReader. */
public class MongoDBReader
        extends Reader
{

    /** Job. */
    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            // the credentials are optional, a MongoDB without access control needs none
            String userName = originalConfig.getString(USERNAME);
            String password = decryptPassword(originalConfig.getString(PASSWORD));
            Configuration connConf = originalConfig.getConfiguration(CONNECTION);
            String database = connConf.getNecessaryValue(DATABASE, REQUIRED_VALUE);
            String authDb = connConf.getString(KeyConstant.MONGO_AUTH_DB, database);
            List<Object> addressList = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            if (originalConfig.getList(COLUMN, Object.class).isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The configuration column is required and must not be empty");
            }
            this.mongoClient = StringUtils.isEmpty(userName) || StringUtils.isEmpty(password)
                    ? MongoUtil.initMongoClient(addressList)
                    : MongoUtil.initCredentialMongoClient(addressList, userName, password, authDb);
        }

        @Override
        public void destroy()
        {
            if (mongoClient != null) {
                mongoClient.close();
                mongoClient = null;
            }
        }
    }

    /**
     * @param password the configured password, possibly an encrypted one
     * @return the password in clear text
     */
    private static String decryptPassword(String password)
    {
        if (password == null || !password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
            return password;
        }
        // the encrypted form is wrapped as ${enc:...}, the cipher text is what the braces hold
        return EncryptUtil.decrypt(password.substring(Constant.ENC_PASSWORD_PREFIX.length(), password.length() - 1));
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {

        private MongoClient mongoClient;

        private String database = null;
        private String collection = null;

        private Document userFilter = null;

        /** Both bounds are extended JSON texts or the {@code min} / {@code max} words for an open end. */
        private String lowerBound = null;
        private String upperBound = null;
        private int fetchSize;

        private MongoRowConverter rowConverter;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = getPluginJobConf();
            String userName = readerSliceConfig.getString(USERNAME);
            String password = decryptPassword(readerSliceConfig.getString(PASSWORD));
            this.fetchSize = readerSliceConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            this.userFilter = MongoUtil.parseFilter(readerSliceConfig.get(KeyConstant.MONGO_QUERY),
                    KeyConstant.MONGO_QUERY);
            this.rowConverter = new MongoRowConverter(parseColumns(readerSliceConfig));
            this.lowerBound = readerSliceConfig.getString(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.getString(KeyConstant.UPPER_BOUND);

            Configuration connConf = readerSliceConfig.getConfiguration(CONNECTION);
            this.database = connConf.getString(DATABASE);
            this.collection = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);
            String authDb = connConf.getString(KeyConstant.MONGO_AUTH_DB, this.database);
            List<Object> addressList = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            this.mongoClient = StringUtils.isEmpty(userName) || StringUtils.isEmpty(password)
                    ? MongoUtil.initMongoClient(addressList)
                    : MongoUtil.initCredentialMongoClient(addressList, userName, password, authDb);
        }

        private static List<String> parseColumns(Configuration readerSliceConfig)
        {
            List<Object> rawColumns = readerSliceConfig.getList(COLUMN, Object.class);
            if (rawColumns.isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The configuration column is required and must not be empty");
            }
            List<String> columns = new ArrayList<>(rawColumns.size());
            for (Object rawColumn : rawColumns) {
                if (!(rawColumn instanceof String column)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The configured column [%s] must be a string", rawColumn));
                }
                columns.add(column);
            }
            return columns;
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            MongoCollection<BsonDocument> bsonCollection = mongoClient.getDatabase(database)
                    .getCollection(this.collection, BsonDocument.class);
            FindIterable<BsonDocument> documents = bsonCollection.find(buildQueryFilter()).batchSize(fetchSize)
                    // a batch handed to a slow writer can idle past the server side cursor timeout
                    .noCursorTimeout(true);
            Bson projection = rowConverter.projection();
            if (projection != null) {
                // read only the configured fields, the constants need no field at all
                documents = documents.projection(projection);
            }

            try (MongoCursor<BsonDocument> cursor = documents.iterator()) {
                while (cursor.hasNext()) {
                    Record record = recordSender.createRecord();
                    rowConverter.processOne(cursor.next(), record);
                    recordSender.sendToWriter(record);
                }
            }
        }

        @Override
        public void destroy()
        {
            if (mongoClient != null) {
                mongoClient.close();
                mongoClient = null;
            }
        }

        /**
         * @return the filter of this slice, never null
         */
        private Document buildQueryFilter()
        {
            if (lowerBound == null || upperBound == null) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The task slice carries no bounds, please check the collection splitting");
            }

            Document filter = new Document();
            if (MongoUtil.MIN_BOUND.equals(lowerBound)) {
                if (!MongoUtil.MAX_BOUND.equals(upperBound)) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID,
                            new Document("$lt", MongoUtil.decodeBound(upperBound)));
                }
            }
            else if (MongoUtil.MAX_BOUND.equals(upperBound)) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID,
                        new Document("$gte", MongoUtil.decodeBound(lowerBound)));
            }
            else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID,
                        new Document("$gte", MongoUtil.decodeBound(lowerBound))
                                .append("$lt", MongoUtil.decodeBound(upperBound)));
            }

            if (userFilter != null && !userFilter.isEmpty()) {
                filter = new Document("$and", List.of(filter, userFilter));
            }

            return filter;
        }
    }
}
