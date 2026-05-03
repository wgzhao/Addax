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


import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.reader.mongodbreader.KeyConstant;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.DATABASE;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

public class CollectionSplitUtil
{
    private CollectionSplitUtil() {}

    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient)
    {

        List<Configuration> confList = new ArrayList<>();

        Configuration connConf = originalSliceConfig.getConfiguration(CONNECTION);
        String dbName = connConf.getString(DATABASE);

        String collectionExpr = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);
        Document queryFilter = parseQueryFilter(originalSliceConfig.getString(KeyConstant.MONGO_QUERY));

        if (null == dbName || dbName.isEmpty() || null == collectionExpr || collectionExpr.isEmpty() || mongoClient == null) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    ILLEGAL_VALUE.getDescription());
        }

        List<String> expandedCollections = CollectionExpandUtil.expandCollectionNames(collectionExpr);
        List<String> availableCollections = getAvailableCollections(mongoClient, dbName, expandedCollections);

        if (availableCollections.isEmpty()) {
            return confList;
        }

        Map<String, Long> docCountCache = new HashMap<>();
        Map<String, Integer> splitPlan = allocateSplitNumberByDocCount(
                mongoClient,
                dbName,
                availableCollections,
                queryFilter,
                docCountCache,
                Math.max(adviceNumber, 1));

        for (String collName : availableCollections) {
            boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);
            int splitNumber = splitPlan.getOrDefault(collName, 1);

            long docCount = docCountCache.getOrDefault(collName, -1L);
            List<Range> rangeList = doSplitCollection(splitNumber, mongoClient, dbName, collName, isObjectId, queryFilter, docCount);
            for (Range range : rangeList) {
                Configuration conf = originalSliceConfig.clone();
                conf.set(CONNECTION + "." + KeyConstant.MONGO_COLLECTION_NAME, collName);
                conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
                conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
                conf.set(KeyConstant.IS_OBJECT_ID, isObjectId);
                confList.add(conf);
            }
        }

        return confList;
    }

    private static List<String> getAvailableCollections(MongoClient mongoClient, String dbName, List<String> expandedCollections)
    {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<String> existingNames = database.listCollectionNames().into(new ArrayList<>());
        List<String> availableCollections = new ArrayList<>();

        for (String collection : expandedCollections) {
            if (existingNames.contains(collection)) {
                availableCollections.add(collection);
            }
        }
        return availableCollections;
    }

    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName)
    {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        if (doc == null) {
            return false;
        }
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        return id instanceof ObjectId;
    }

    private static Map<String, Integer> allocateSplitNumberByDocCount(MongoClient mongoClient,
            String dbName,
            List<String> collections,
            Document queryFilter,
            Map<String, Long> docCountCache,
            int adviceNumber)
    {
        Map<String, Integer> splitPlan = new HashMap<>();
        if (collections.isEmpty()) {
            return splitPlan;
        }

        int totalCollectionCount = collections.size();
        int totalTaskCount = Math.max(adviceNumber, totalCollectionCount);
        int extraTaskCount = totalTaskCount - totalCollectionCount;

        for (String collection : collections) {
            splitPlan.put(collection, 1);
        }

        // No extra split is needed, avoid any expensive counting.
        if (extraTaskCount <= 0) {
            return splitPlan;
        }

        long totalDocCount = 0L;
        for (String collection : collections) {
            long count = Math.max(fetchDocCount(mongoClient, dbName, collection, queryFilter), 0L);
            docCountCache.put(collection, count);
            totalDocCount += count;
        }

        if (totalDocCount == 0L) {
            for (int i = 0; i < extraTaskCount; i++) {
                String collection = collections.get(i % totalCollectionCount);
                splitPlan.put(collection, splitPlan.get(collection) + 1);
            }
            return splitPlan;
        }

        List<Quota> quotas = new ArrayList<>();
        int assigned = 0;
        for (String collection : collections) {
            double exact = 1.0D * extraTaskCount * docCountCache.get(collection) / totalDocCount;
            int floor = (int) Math.floor(exact);
            assigned += floor;
            splitPlan.put(collection, splitPlan.get(collection) + floor);
            quotas.add(new Quota(collection, exact - floor));
        }

        int remain = extraTaskCount - assigned;
        quotas.sort((o1, o2) -> Double.compare(o2.remainder, o1.remainder));
        for (int i = 0; i < remain; i++) {
            Quota quota = quotas.get(i % quotas.size());
            splitPlan.put(quota.collection, splitPlan.get(quota.collection) + 1);
        }
        return splitPlan;
    }

    private static long fetchDocCount(MongoClient mongoClient, String dbName, String collName, Document queryFilter)
    {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        try {
            if (hasQueryFilter(queryFilter)) {
                return database.getCollection(collName).countDocuments(queryFilter);
            }
            Document result = database.runCommand(new Document("collStats", collName));
            Object count = result.get("count");
            if (count instanceof Integer) {
                return ((Integer) count).longValue();
            }
            if (count instanceof Long) {
                return (Long) count;
            }
            if (count instanceof Double) {
                return ((Double) count).longValue();
            }
            return 0L;
        }
        catch (Exception e) {
            return 0L;
        }
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
            String dbName, String collName, boolean isObjectId, Document queryFilter, long knownDocCount)
    {

        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        List<Range> rangeList = new ArrayList<>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            rangeList.add(range);
            return rangeList;
        }

        long docCount = knownDocCount >= 0 ? knownDocCount : fetchDocCount(mongoClient, dbName, collName, queryFilter);
        if (docCount == 0) {
            return rangeList;
        }
        if (adviceNumber > docCount) {
            adviceNumber = (int) docCount;
        }
        if (adviceNumber <= 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            rangeList.add(range);
            return rangeList;
        }

        Document result = database.runCommand(new Document("collStats", collName));
        int avgObjSize = 1;
        Object avgObjSizeObj = result.get("avgObjSize");
        if (avgObjSizeObj instanceof Integer) {
            avgObjSize = (Integer) avgObjSizeObj;
        }
        else if (avgObjSizeObj instanceof Double) {
            avgObjSize = ((Double) avgObjSizeObj).intValue();
        }
        int splitPointCount = adviceNumber - 1;
        long chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<>();
        boolean supportSplitVector = !hasQueryFilter(queryFilter);

        // test if user has splitVector role(clusterManager)
        if (supportSplitVector) {
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
                else {
                    supportSplitVector = false;
                }
            }
        }

        if (supportSplitVector) {
            try {
                boolean forceMedianSplit = false;
                long maxChunkSize = (docCount / splitPointCount - 1) * 2L * avgObjSize / (1024 * 1024);
                // splitVector can use storage metadata to avoid a full scan when query is absent.
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
                List<?> splitKeys = result.get("splitKeys", List.class);

                if (splitKeys != null) {
                    for (Object splitKeyObj : splitKeys) {
                        Document splitKey = (Document) splitKeyObj;
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
            }
            catch (Exception e) {
                splitPoints.clear();
            }
        }

        if (splitPoints.isEmpty()) {
            splitPoints.addAll(sampleSplitPointsSequentially(col, queryFilter, splitPointCount, chunkDocCount, isObjectId));
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

    private static List<Object> sampleSplitPointsSequentially(MongoCollection<Document> collection,
            Document queryFilter,
            int splitPointCount,
            long chunkDocCount,
            boolean isObjectId)
    {
        List<Object> splitPoints = new ArrayList<>();
        if (splitPointCount <= 0 || chunkDocCount <= 0) {
            return splitPoints;
        }

        Document filter = hasQueryFilter(queryFilter) ? queryFilter : new Document();
        long nextSplitIndex = chunkDocCount + 1;
        long currentIndex = 0;

        // A single ordered cursor avoids repeated large-offset skips on oversized collections.
        try (MongoCursor<Document> cursor = collection.find(filter)
                .projection(new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                .sort(new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                .batchSize(1024)
                .iterator()) {
            while (cursor.hasNext() && splitPoints.size() < splitPointCount) {
                Document doc = cursor.next();
                currentIndex++;
                if (currentIndex < nextSplitIndex) {
                    continue;
                }

                Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    splitPoints.add(((ObjectId) id).toHexString());
                }
                else {
                    splitPoints.add(id);
                }
                nextSplitIndex += chunkDocCount;
            }
        }

        return splitPoints;
    }

    private static Document parseQueryFilter(String query)
    {
        if (query == null || query.trim().isEmpty()) {
            return null;
        }
        return Document.parse(query);
    }

    private static boolean hasQueryFilter(Document queryFilter)
    {
        return queryFilter != null && !queryFilter.isEmpty();
    }
}

class Quota
{
    String collection;
    double remainder;

    Quota(String collection, double remainder)
    {
        this.collection = collection;
        this.remainder = remainder;
    }
}

class Range
{
    Object lowerBound;
    Object upperBound;
}
