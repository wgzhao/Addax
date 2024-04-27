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

import com.google.common.base.Strings;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.reader.mongodbreader.KeyConstant;
import com.wgzhao.addax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.alibaba.fastjson2.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectionSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CollectionSplitUtil.class);

    public static List<Configuration> doSplit(
        Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient, String query) {
        LOG.info("adviceNumber is :" + adviceNumber);
        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw AddaxException.asAddaxException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
        for (Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.RANGE, JSONObject.toJSONString(range));
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }

    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        if (id instanceof ObjectId) {
            return true;
        }
        return false;
    }

    /**
     * 通过MongodbSampleSplitter、MongodbCommonSplitter等2个类按照优先顺序进行切片
     *
     * @param adviceNumber
     * @param mongoClient
     * @param dbName
     * @param collName
     * @param isObjectId
     * @param query
     * @return
     */
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient, String dbName, String collName, boolean isObjectId, String query) {

        List<Range> rangeList = null;
        long starttime = System.currentTimeMillis();
        long endtime = 0L;
        try {

            rangeList = MongodbSampleSplitter.split(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
            endtime = System.currentTimeMillis();
            LOG.info("成功通过Sample采样进行切片,切片操作耗时:{}毫秒", (endtime - starttime));

        } catch (Exception e) {
            rangeList = null;
            LOG.info(e.getMessage());
            e.printStackTrace();
        }


        if (null == rangeList) {
            starttime = System.currentTimeMillis();
            rangeList = MongodbCommonSplitter.split(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
            endtime = System.currentTimeMillis();
            LOG.info("成功通过CommonSplit进行切片,切片操作耗时:{}毫秒", (endtime - starttime));

        }

        return rangeList;
    }

    public static List<Range> getOneSplit() {
        Range range = new Range();
        range.setLowerBound("min");
        range.setUpperBound("max");
        range.setSampleType(true);
        return Arrays.asList(range);
    }


    // bson转json格式设置
    public static JsonWriterSettings getJsonWriterSettings() {
        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
            .dateTimeConverter((value, writer) -> writer.writeString(Long.toString(value)))
            .decimal128Converter((value, writer) -> writer.writeNumber(value.toString()))
            .objectIdConverter((value, writer) -> writer.writeString(value.toString()))
            .int32Converter((value, writer) -> writer.writeNumber(value.toString()))
            .int64Converter((value, writer) -> writer.writeString(Long.toString(value)))
            .build();

        return jsonWriterSettings;
    }
}
