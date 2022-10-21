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

package com.wgzhao.addax.plugin.reader.mongodbreader.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.reader.mongodbreader.KeyConstant;
import com.wgzhao.addax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.base.Key.CONNECTION;
import static com.wgzhao.addax.common.base.Key.DATABASE;

public class CollectionSplitUtil
{

    private CollectionSplitUtil() {}

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient)
    {

        List<Configuration> confList = new ArrayList<>();

        Configuration connConf = Configuration.from(originalSliceConfig.getList(CONNECTION, Object.class).get(0).toString());
        String dbName = connConf.getString(DATABASE);

        String collName = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if (null == dbName || dbName.isEmpty() || null == collName || collName.isEmpty() || mongoClient == null) {
            throw AddaxException.asAddaxException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId);
        for (Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECT_ID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }

    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName)
    {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        assert doc != null;
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        return id instanceof ObjectId;
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
            String dbName, String collName, boolean isObjectId)
    {

        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            rangeList.add(range);
            return rangeList;
        }

        Document result = database.runCommand(new Document("collStats", collName));
        int docCount = result.getInteger("count");
        if (docCount == 0) {
            return rangeList;
        }
        int avgObjSize = 1;
        Object avgObjSizeObj = result.get("avgObjSize");
        if (avgObjSizeObj instanceof Integer) {
            avgObjSize = (Integer) avgObjSizeObj;
        }
        else if (avgObjSizeObj instanceof Double) {
            avgObjSize = ((Double) avgObjSizeObj).intValue();
        }
        int splitPointCount = adviceNumber - 1;
        int chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<>();

        // test if user has splitVector role(clusterManager)
        boolean supportSplitVector = true;
        try {
            database.runCommand(new Document("splitVector", dbName + "." + collName)
                    .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                    .append("force", true));
        }
        catch (MongoCommandException e) {
            if (e.getErrorCode() == KeyConstant.MONGO_UNAUTHORIZED_ERR_CODE ||
                    e.getErrorCode() == KeyConstant.MONGO_ILLEGAL_OP_ERR_CODE ||
                    e.getErrorCode() == KeyConstant.MONGO_COMMAND_NOT_FOUND_CODE) {
                supportSplitVector = false;
            }
        }

        if (supportSplitVector) {
            boolean forceMedianSplit = false;
            int maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
            //int maxChunkSize = (chunkDocCount - 1) * 2 * avgObjSize / (1024 * 1024)
            if (maxChunkSize < 1) {
                forceMedianSplit = true;
            }
            if (!forceMedianSplit) {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("maxChunkSize", maxChunkSize)
                        .append("maxSplitPoints", adviceNumber - 1));
            }
            else {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("force", true));
            }
            ArrayList<Document> splitKeys = result.get("splitKeys", ArrayList.class);

            for (Document splitKey : splitKeys) {
                Object id = splitKey.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId) id;
                    splitPoints.add(oid.toHexString());
                }
                else {
                    splitPoints.add(id);
                }
            }
        }
        else {
            int skipCount = chunkDocCount;
            MongoCollection<Document> col = database.getCollection(collName);

            for (int i = 0; i < splitPointCount; i++) {
                Document doc = col.find().skip(skipCount).limit(chunkDocCount).first();
                assert doc != null;
                Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId) id;
                    splitPoints.add(oid.toHexString());
                }
                else {
                    splitPoints.add(id);
                }
                skipCount += chunkDocCount;
            }
        }

        Object lastObjectId = "min";
        for (Object splitPoint : splitPoints) {
            Range range = new Range();
            range.lowerBound = lastObjectId;
            lastObjectId = splitPoint;
            range.upperBound = lastObjectId;
            rangeList.add(range);
        }
        Range range = new Range();
        range.lowerBound = lastObjectId;
        range.upperBound = "max";
        rangeList.add(range);

        return rangeList;
    }
}

class Range
{
    Object lowerBound;
    Object upperBound;
}
