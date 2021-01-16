package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class CollectionSplitUtil
{

    private CollectionSplitUtil() {}

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient)
    {

        List<Configuration> confList = new ArrayList<>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if (null == dbName || dbName.isEmpty() || null == collName || collName.isEmpty() || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId);
        for (Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
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
            return Collections.singletonList(range);
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
                    e.getErrorCode() == KeyConstant.MONGO_ILLEGALOP_ERR_CODE ||
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
