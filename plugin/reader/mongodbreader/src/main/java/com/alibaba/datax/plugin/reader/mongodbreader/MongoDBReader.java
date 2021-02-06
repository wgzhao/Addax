package com.alibaba.datax.plugin.reader.mongodbreader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.util.stream.Collectors.toList;

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
            this.originalConfig = super.getPluginJobConf();
            String userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            String password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
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
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private MongoClient mongoClient;

        private String database = null;
        private String collection = null;

        private String query = null;

        private List<String> mongodbColumn = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        private boolean isNullOrEmpty(String obj)
        {
            return obj == null || obj.isEmpty();
        }

        private void setMongoColumn(MongoClient client, String db, String tbl)
        {
            Document item = client.getDatabase(db).getCollection(tbl).find().first();
            assert item != null;
            this.mongodbColumn = item.keySet().stream().filter(x -> !"_id".equals(x)).collect(toList());
        }

        @Override
        public void startRead(RecordSender recordSender)
        {

            if (lowerBound == null || upperBound == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumn == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
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
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }
            if (!isNullOrEmpty((query))) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }

            LOG.info("column info {}", mongodbColumn);

            dbCursor = col.find(filter).iterator();

            while (dbCursor.hasNext()) {
                Record record = recordSender.createRecord();
                Document item = dbCursor.next();
                for (String field : mongodbColumn) {
                    Object tempCol = item.get(field);
                    if (tempCol == null) {
                        record.addColumn(new StringColumn(null));
                        continue;
                    }
                    switch (tempCol.getClass().getSimpleName())
                    {
                        case "Boolean":
                            record.addColumn(new BoolColumn((Boolean) tempCol));
                            break;
                        case "Integer":
                            record.addColumn(new LongColumn((Integer) tempCol));
                            break;
                        case "Long":
                            record.addColumn(new LongColumn((Long) tempCol));
                            break;
                        case "Double":
                            record.addColumn(new DoubleColumn((Double) tempCol));
                            break;
                        case "Date":
                            record.addColumn(new DateColumn((Date) tempCol));
                            break;
                        case "Document":
                            record.addColumn(new StringColumn(((Document) tempCol).toJson()));
                            break;
                        case "ArrayList":
                            ArrayList<String> array = (ArrayList<String>) tempCol;
                            String tempArrayStr = "{" + String.join(",", array) + "}";
                            record.addColumn(new StringColumn(tempArrayStr));
                            break;
                        default:
                            record.addColumn(new StringColumn(tempCol.toString()));
                            break;
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            String userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            String password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            if (!isNullOrEmpty((userName)) && !isNullOrEmpty((password))) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName, password, authDb);
            }
            else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumn = readerSliceConfig.getList(KeyConstant.MONGO_COLUMN, String.class);
            if (mongodbColumn.size() == 1 && mongodbColumn.get(0).equals("*")) {
                setMongoColumn(mongoClient, database, collection);
            }
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
