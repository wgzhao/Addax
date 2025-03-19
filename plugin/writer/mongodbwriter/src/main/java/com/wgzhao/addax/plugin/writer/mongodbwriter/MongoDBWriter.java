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

package com.wgzhao.addax.plugin.writer.mongodbwriter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.EncryptUtil;
import com.wgzhao.addax.plugin.writer.mongodbwriter.util.MongoUtil;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.CONNECTION;
import static com.wgzhao.addax.common.base.Key.DATABASE;
import static com.wgzhao.addax.common.base.Key.PASSWORD;
import static com.wgzhao.addax.common.base.Key.PRE_SQL;
import static com.wgzhao.addax.common.base.Key.USERNAME;
import static com.wgzhao.addax.common.base.Key.WRITE_MODE;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class MongoDBWriter
        extends Writer
{

    public static class Job
            extends Writer.Job
    {

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> configList = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare()
        {
            super.prepare();
            // parameters check
            originalConfig.getNecessaryValue(CONNECTION, REQUIRED_VALUE);
            Configuration connConf = originalConfig.getConfiguration(CONNECTION);
            List<Object> address = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            if (address == null || address.isEmpty()) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        "The configuration address is illegal, please check your json file:"
                );
            }

            String dbName = connConf.getNecessaryValue(DATABASE, REQUIRED_VALUE);
            String collection = connConf.getNecessaryValue(KeyConstant.MONGO_COLLECTION_NAME, REQUIRED_VALUE);
            String username = connConf.getString(USERNAME);
            String password = connConf.getString(PASSWORD);
            if (password != null && password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
                // encrypted password, need to decrypt
                password = EncryptUtil.decrypt(password.substring(6, password.length() - 1));
            }
            MongoClient mongoClient;
            if (StringUtils.isEmpty((username)) || StringUtils.isEmpty((password))) {
                mongoClient = MongoUtil.initMongoClient(address);
            }
            else {
                mongoClient = MongoUtil.initCredentialMongoClient(address, username, password, dbName);
            }

            String preSqls = connConf.getString(PRE_SQL);
            if (StringUtils.isNotBlank(preSqls)) {
                executePreSql(mongoClient, dbName, collection, Configuration.from(preSqls));
            }
        }

        private void executePreSql(MongoClient mongoClient, String database, String collection, Configuration preSql)
        {

            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(collection);
            String type = preSql.getString("type");
            if (StringUtils.isBlank(type)) {
                return;
            }

            if (type.equals("drop")) {
                col.drop();
            }
            else if (type.equals("remove")) {
                String json = preSql.getString("json");
                BasicDBObject query;
                if (!StringUtils.isBlank(json)) {
                    query = new BasicDBObject();
                    List<Object> items = preSql.getList("item", Object.class);
                    for (Object con : items) {
                        Configuration _conf = Configuration.from(con.toString());
                        if (StringUtils.isBlank((_conf.getString("condition")))) {
                            query.put(_conf.getString("name"), _conf.get("value"));
                        }
                        else {
                            query.put(_conf.getString("name"),
                                    new BasicDBObject(_conf.getString("condition"), _conf.get("value")));
                        }
                    }
                }
                else {
                    query = (BasicDBObject) JSON.parse(json);
                }
                col.deleteMany(query);
            }
        }

        @Override
        public void destroy()
        {

        }
    }

    public static class Task
            extends Writer.Task
    {

        private MongoClient mongoClient;

        private String database = null;
        private String collection = null;
        private Integer batchSize = null;
        private JSONArray mongodbColumnMeta = null;
        private String writeMode = null;
        private String updateKey;

        private boolean isNullOrEmpty(String obj)
        {
            return obj == null || obj.isEmpty();
        }

        @Override
        public void init()
        {
            Configuration writerSliceConfig = this.getPluginJobConf();
            String userName = writerSliceConfig.getString(USERNAME);
            String password = writerSliceConfig.getString(PASSWORD);
            if (password != null && password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
                // encrypted password, need to decrypt
                password = EncryptUtil.decrypt(password.substring(6, password.length() - 1));
            }
            Configuration connConf = writerSliceConfig.getConfiguration(CONNECTION);
            this.database = connConf.getString(DATABASE);
            List<Object> addressList = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            if (!isNullOrEmpty((userName)) && !isNullOrEmpty((password))) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(addressList, userName, password, database);
            }
            else {
                this.mongoClient = MongoUtil.initMongoClient(addressList);
            }
            this.collection = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = writerSliceConfig.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
            this.mongodbColumnMeta = JSON.parseArray(writerSliceConfig.getString(COLUMN));
            this.writeMode = writerSliceConfig.getString(WRITE_MODE, "insert");
            if (this.writeMode.startsWith("update")) {
                if (!this.writeMode.contains("(")) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "When specifying the mode is update, you MUST both specify the field to be updated");
                }
                this.updateKey = this.writeMode.split("\\(")[1].replace(")", "");
                this.writeMode = "update";
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<BasicDBObject> col = db.getCollection(this.collection, BasicDBObject.class);
            List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(col, writerBuffer, mongodbColumnMeta);
                    writerBuffer.clear();
                }
            }
            if (!writerBuffer.isEmpty()) {
                doBatchInsert(col, writerBuffer, mongodbColumnMeta);
                writerBuffer.clear();
            }
        }

        private void doBatchInsert(MongoCollection<BasicDBObject> collection, List<Record> writerBuffer, JSONArray columnMeta)
        {

            List<BasicDBObject> dataList = new ArrayList<>();

            for (Record record : writerBuffer) {

                BasicDBObject data = new BasicDBObject();

                for (int i = 0; i < record.getColumnNumber(); i++) {

                    String type = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_TYPE);
                    String name = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME);
                    //空记录处理
                    if (isNullOrEmpty((record.getColumn(i)).asString())) {
                        if (KeyConstant.isArrayType(type.toLowerCase())) {
                            data.put(name, new Object[0]);
                        }
                        else {
                            data.put(name, record.getColumn(i).asString());
                        }
                        continue;
                    }
                    if (Column.Type.INT.name().equalsIgnoreCase(type)) {
                        // the int type is special
                        try {
                            data.put(name, Integer.parseInt(String.valueOf(record.getColumn(i).getRawData())));
                        }
                        catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        }
                    }
                    else if (record.getColumn(i) instanceof StringColumn) {
                        //handle ObjectId and array type
                        try {
                            if (KeyConstant.isObjectIdType(type.toLowerCase())) {
                                data.put(name, new ObjectId(record.getColumn(i).asString()));
                            }
                            else if (KeyConstant.isArrayType(type.toLowerCase())) {
                                String splitter = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_SPLITTER);
                                if (isNullOrEmpty((splitter))) {
                                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                            ILLEGAL_VALUE.getDescription());
                                }
                                String itemType = columnMeta.getJSONObject(i).getString(KeyConstant.ITEM_TYPE);
                                if (itemType != null && !itemType.isEmpty()) {
                                    String[] item = record.getColumn(i).asString().split(splitter);
                                    if (itemType.equalsIgnoreCase(Column.Type.DOUBLE.name())) {
                                        ArrayList<Double> list = new ArrayList<>();
                                        for (String s : item) {
                                            list.add(Double.parseDouble(s));
                                        }
                                        data.put(name, list.toArray(new Double[0]));
                                    }
                                    else if (itemType.equalsIgnoreCase(Column.Type.INT.name())) {
                                        ArrayList<Integer> list = new ArrayList<>();
                                        for (String s : item) {
                                            list.add(Integer.parseInt(s));
                                        }
                                        data.put(name, list.toArray(new Integer[0]));
                                    }
                                    else if (itemType.equalsIgnoreCase(Column.Type.LONG.name())) {
                                        ArrayList<Long> list = new ArrayList<>();
                                        for (String s : item) {
                                            list.add(Long.parseLong(s));
                                        }
                                        data.put(name, list.toArray(new Long[0]));
                                    }
                                    else if (itemType.equalsIgnoreCase(Column.Type.BOOL.name())) {
                                        ArrayList<Boolean> list = new ArrayList<>();
                                        for (String s : item) {
                                            list.add(Boolean.parseBoolean(s));
                                        }
                                        data.put(name, list.toArray(new Boolean[0]));
                                    }
                                    else if (itemType.equalsIgnoreCase(Column.Type.BYTES.name())) {
                                        ArrayList<Byte> list = new ArrayList<>();
                                        for (String s : item) {
                                            list.add(Byte.parseByte(s));
                                        }
                                        data.put(name, list.toArray(new Byte[0]));
                                    }
                                    else {
                                        data.put(name, record.getColumn(i).asString().split(splitter));
                                    }
                                }
                                else {
                                    data.put(name, record.getColumn(i).asString().split(splitter));
                                }
                            }
                            else if (type.equalsIgnoreCase("json")) {
                                Object mode = JSON.parse(record.getColumn(i).asString());
                                data.put(name, JSON.toJSON(mode));
                            }
                            else {
                                data.put(name, record.getColumn(i).asString());
                            }
                        }
                        catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        }
                    }
                    else if (record.getColumn(i) instanceof LongColumn) {

                        if (Column.Type.LONG.name().equalsIgnoreCase(type)) {
                            data.put(name, record.getColumn(i).asLong());
                        }
                        else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }
                    }
                    else if (record.getColumn(i) instanceof DateColumn) {

                        if (Column.Type.DATE.name().equalsIgnoreCase(type)) {
                            data.put(name, record.getColumn(i).asDate());
                        }
                        else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }
                    }
                    else if (record.getColumn(i) instanceof DoubleColumn) {

                        if (Column.Type.DOUBLE.name().equalsIgnoreCase(type)) {
                            data.put(name, record.getColumn(i).asDouble());
                        }
                        else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }
                    }
                    else if (record.getColumn(i) instanceof BoolColumn) {

                        if (Column.Type.BOOL.name().equalsIgnoreCase(type)) {
                            data.put(name, record.getColumn(i).asBoolean());
                        }
                        else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }
                    }
                    else if (record.getColumn(i) instanceof BytesColumn) {

                        if (Column.Type.BYTES.name().equalsIgnoreCase(type)) {
                            data.put(name, record.getColumn(i).asBytes());
                        }
                        else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }
                    }
                    else {
                        data.put(name, record.getColumn(i).asString());
                    }
                }
                dataList.add(data);
            }

            if ("update".equals(writeMode)) {
                List<ReplaceOneModel<BasicDBObject>> replaceOneModelList = new ArrayList<>();
                for (BasicDBObject data : dataList) {
                    BasicDBObject query = new BasicDBObject();
                    query.put(updateKey, data.get(updateKey));
                    ReplaceOneModel<BasicDBObject> replaceOneModel = new ReplaceOneModel<>(query, data, new ReplaceOptions().upsert(true));
                    replaceOneModelList.add(replaceOneModel);
                }
                collection.bulkWrite(replaceOneModelList, new BulkWriteOptions().ordered(false));
            }
            else {
                collection.insertMany(dataList);
            }
        }

        @Override
        public void destroy()
        {
            mongoClient.close();
        }
    }
}
