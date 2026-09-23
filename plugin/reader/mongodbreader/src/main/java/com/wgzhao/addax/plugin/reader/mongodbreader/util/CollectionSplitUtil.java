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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.DATABASE;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/** Collection Split Util. */
public class CollectionSplitUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(CollectionSplitUtil.class);

    /** The document count is unknown, the collection must not be split blindly. */
    private static final long UNKNOWN_DOC_COUNT = -1L;

    private CollectionSplitUtil() {}

    /** Dosplit. */
    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient)
    {
        List<Configuration> confList = new ArrayList<>();

        Configuration connConf = originalSliceConfig.getConfiguration(CONNECTION);
        String dbName = connConf.getString(DATABASE);
        String collectionExpr = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);
        Document queryFilter = MongoUtil.parseFilter(originalSliceConfig.get(KeyConstant.MONGO_QUERY),
                KeyConstant.MONGO_QUERY);

        if (StringUtils.isBlank(dbName) || StringUtils.isBlank(collectionExpr) || mongoClient == null) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE, ILLEGAL_VALUE.getDescription());
        }

        List<String> expandedCollections = CollectionExpandUtil.expandCollectionNames(collectionExpr);
        List<String> availableCollections = getAvailableCollections(mongoClient, dbName, expandedCollections);
        if (availableCollections.isEmpty()) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("None of the collections [%s] exists in the database [%s]",
                            String.join(", ", expandedCollections), dbName));
        }

        Map<String, Long> docCountCache = new HashMap<>();
        MongoDatabase database = mongoClient.getDatabase(dbName);
        Map<String, Integer> splitPlan = allocateSplitNumberByDocCount(
                database,
                availableCollections,
                queryFilter,
                docCountCache,
                Math.max(adviceNumber, 1));

        for (String collName : availableCollections) {
            int splitNumber = splitPlan.getOrDefault(collName, 1);
            long docCount = docCountOf(docCountCache, database, collName, queryFilter);

            List<Range> rangeList = doSplitCollection(splitNumber, database, collName, queryFilter, docCount);
            for (Range range : rangeList) {
                Configuration conf = originalSliceConfig.clone();
                conf.set(CONNECTION + "." + KeyConstant.MONGO_COLLECTION_NAME, collName);
                conf.set(KeyConstant.LOWER_BOUND, range.lowerBound());
                conf.set(KeyConstant.UPPER_BOUND, range.upperBound());
                confList.add(conf);
            }
        }

        if (confList.isEmpty()) {
            // every collection was empty, keep one slice so the job still succeeds with no record
            // instead of failing on a reader that handed back no task at all
            Configuration conf = originalSliceConfig.clone();
            conf.set(CONNECTION + "." + KeyConstant.MONGO_COLLECTION_NAME, availableCollections.get(0));
            conf.set(KeyConstant.LOWER_BOUND, MongoUtil.MIN_BOUND);
            conf.set(KeyConstant.UPPER_BOUND, MongoUtil.MAX_BOUND);
            confList.add(conf);
        }

        return confList;
    }

    private static List<String> getAvailableCollections(MongoClient mongoClient, String dbName, List<String> expandedCollections)
    {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        Set<String> existingNames = new HashSet<>(database.listCollectionNames().into(new ArrayList<>()));
        List<String> availableCollections = new ArrayList<>();

        for (String collection : expandedCollections) {
            if (existingNames.contains(collection)) {
                availableCollections.add(collection);
            }
            else {
                LOG.warn("The collection [{}] does not exist in the database [{}], skip it", collection, dbName);
            }
        }
        return availableCollections;
    }

    /**
     * Share the requested number of tasks over the collections in proportion to their document
     * count, so a small collection does not take tasks away from a large one.
     */
    private static Map<String, Integer> allocateSplitNumberByDocCount(
            MongoDatabase database,
            List<String> collections,
            Document queryFilter,
            Map<String, Long> docCountCache,
            int adviceNumber)
    {
        Map<String, Integer> splitPlan = new HashMap<>();
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
            // an unknown count takes no share of the ratio, it keeps its single task
            totalDocCount += Math.max(docCountOf(docCountCache, database, collection, queryFilter), 0L);
        }

        if (totalDocCount == 0L) {
            // nothing is known about any collection, hand the extra tasks out evenly
            for (int i = 0; i < extraTaskCount; i++) {
                String collection = collections.get(i % totalCollectionCount);
                splitPlan.put(collection, splitPlan.get(collection) + 1);
            }
            return splitPlan;
        }

        List<Quota> quotas = new ArrayList<>();
        int assigned = 0;
        for (String collection : collections) {
            // an unknown count is unknown, not negative, so it must not take tasks away from the others
            double exact = 1.0D * extraTaskCount
                    * Math.max(docCountCache.getOrDefault(collection, 0L), 0L) / totalDocCount;
            int floor = (int) Math.floor(exact);
            assigned += floor;
            splitPlan.put(collection, Math.max(splitPlan.get(collection) + floor, 1));
            quotas.add(new Quota(collection, exact - floor));
        }

        int remain = extraTaskCount - assigned;
        quotas.sort((o1, o2) -> Double.compare(o2.remainder(), o1.remainder()));
        for (int i = 0; i < remain; i++) {
            Quota quota = quotas.get(i % quotas.size());
            splitPlan.put(quota.collection(), splitPlan.get(quota.collection()) + 1);
        }
        return splitPlan;
    }

    /**
     * @return the number of documents of the collection, or {@link #UNKNOWN_DOC_COUNT} when the
     * server refuses to count. A refused count is remembered, retrying it would fail the same way.
     */
    private static long docCountOf(Map<String, Long> docCountCache, MongoDatabase database,
            String collName, Document queryFilter)
    {
        Long cached = docCountCache.get(collName);
        if (cached != null) {
            return cached;
        }
        long docCount = fetchDocCount(database, collName, queryFilter);
        docCountCache.put(collName, docCount);
        return docCount;
    }

    /**
     * @return the number of documents, or {@link #UNKNOWN_DOC_COUNT} when the server refuses to count
     */
    private static long fetchDocCount(MongoDatabase database, String collName, Document queryFilter)
    {
        MongoCollection<Document> col = database.getCollection(collName);
        try {
            // estimatedDocumentCount reads collection metadata instead of scanning the documents
            return hasQueryFilter(queryFilter) ? col.countDocuments(queryFilter) : col.estimatedDocumentCount();
        }
        catch (Exception e) {
            LOG.warn("Failed to count the documents of the collection [{}], treat it as unknown. reason: {}",
                    collName, e.getMessage());
            return UNKNOWN_DOC_COUNT;
        }
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoDatabase database,
            String collName, Document queryFilter, long docCount)
    {
        MongoCollection<Document> col = database.getCollection(collName);

        if (adviceNumber <= 1) {
            return List.of(new Range(MongoUtil.MIN_BOUND, MongoUtil.MAX_BOUND));
        }
        if (docCount == UNKNOWN_DOC_COUNT) {
            // a wrong split boundary silently loses records, read the whole collection instead
            return List.of(new Range(MongoUtil.MIN_BOUND, MongoUtil.MAX_BOUND));
        }
        if (docCount == 0) {
            LOG.warn("Skip the collection [{}], it holds no document matching the query", collName);
            return List.of();
        }

        int splitNumber = (int) Math.min(adviceNumber, docCount);
        if (splitNumber <= 1) {
            return List.of(new Range(MongoUtil.MIN_BOUND, MongoUtil.MAX_BOUND));
        }

        int splitPointCount = splitNumber - 1;
        long chunkDocCount = docCount / splitNumber;

        List<Object> splitPoints = hasQueryFilter(queryFilter)
                ? List.of()
                : fetchSplitPointsFromServer(database, collName, adviceNumber, splitPointCount, docCount);
        if (splitPoints.isEmpty()) {
            splitPoints = sampleSplitPointsSequentially(col, queryFilter, splitPointCount, chunkDocCount);
        }

        List<Range> rangeList = new ArrayList<>(splitPoints.size() + 1);
        String lowerBound = MongoUtil.MIN_BOUND;
        for (Object splitPoint : splitPoints) {
            String upperBound = MongoUtil.encodeBound(splitPoint);
            rangeList.add(new Range(lowerBound, upperBound));
            lowerBound = upperBound;
        }
        rangeList.add(new Range(lowerBound, MongoUtil.MAX_BOUND));
        return rangeList;
    }

    /**
     * Ask the server for split points. splitVector is an internal command that needs a privileged
     * account and a shard aware deployment, so a refusal only means sampling the collection instead.
     */
    private static List<Object> fetchSplitPointsFromServer(MongoDatabase database, String collName,
            int adviceNumber, int splitPointCount, long docCount)
    {
        Document splitVector = new Document("splitVector", database.getName() + "." + collName)
                .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1));
        try {
            // collStats is deprecated since MongoDB 6.2 and may be denied on hosted clusters
            Document stats = database.runCommand(new Document("collStats", collName));
            long avgObjSize = stats.get("avgObjSize") instanceof Number number ? number.longValue() : 1L;
            // aim at one chunk per split point, maxSplitPoints already keeps the count in hand.
            // Rounding up keeps the command cheap: a forced split scans the whole collection and
            // still returns a single split point, while a chunk target below a megabyte returns
            // none, which leaves the sampling below to hand out the requested number of tasks.
            long maxChunkSize = (docCount / splitPointCount * avgObjSize + (1024 * 1024 - 1)) / (1024 * 1024);
            if (maxChunkSize > 0) {
                splitVector.append("maxChunkSize", maxChunkSize).append("maxSplitPoints", adviceNumber - 1);
            }
            else {
                splitVector.append("force", true);
            }

            Document result = database.runCommand(splitVector);
            List<?> splitKeys = result.get("splitKeys", List.class);
            if (splitKeys == null) {
                return List.of();
            }

            List<Object> splitPoints = new ArrayList<>(splitKeys.size());
            for (Object splitKey : splitKeys) {
                splitPoints.add(((Document) splitKey).get(KeyConstant.MONGO_PRIMARY_ID));
            }
            return splitPoints;
        }
        catch (MongoCommandException | ClassCastException e) {
            LOG.warn("Failed to split the collection [{}] by the server side splitVector, sample it instead. reason: {}",
                    collName, e.getMessage());
            return List.of();
        }
    }

    private static List<Object> sampleSplitPointsSequentially(MongoCollection<Document> collection,
            Document queryFilter,
            int splitPointCount,
            long chunkDocCount)
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

                splitPoints.add(doc.get(KeyConstant.MONGO_PRIMARY_ID));
                nextSplitIndex += chunkDocCount;
            }
        }

        return splitPoints;
    }

    private static boolean hasQueryFilter(Document queryFilter)
    {
        return queryFilter != null && !queryFilter.isEmpty();
    }

    /**
     * A range of the primary key read by one task. Both bounds are extended JSON, see
     * {@link MongoUtil#encodeBound(Object)}, or the {@code min} / {@code max} words for an open end.
     */
    private record Range(String lowerBound, String upperBound) {}

    /** The fractional part of a collection's share of the extra tasks. */
    private record Quota(String collection, double remainder) {}
}
