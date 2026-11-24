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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.element.Column;
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
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.core.base.Key.BATCH_SIZE;
import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.DATABASE;
import static com.wgzhao.addax.core.base.Key.PASSWORD;
import static com.wgzhao.addax.core.base.Key.PRE_SQL;
import static com.wgzhao.addax.core.base.Key.USERNAME;
import static com.wgzhao.addax.core.base.Key.WRITE_MODE;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Mongo DBWriter. */
public class MongoDBWriter
        extends Writer
{
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBWriter.class);

    /**
     * The whole writeMode value has to match exactly. A prefix-only check would accept trailing
     * text after the closing parenthesis and let update(id)) produce the key id), so the pattern
     * excludes parentheses and commas inside the parameters.
     */
    private static final Pattern UPDATE_MODE_PATTERN =
            Pattern.compile("(?i)^update\\(\\s*([^(),]+?)\\s*\\)$");
    private static final Pattern MERGE_MODE_PATTERN =
            Pattern.compile("(?i)^merge\\(\\s*([^(),]+?)\\s*,\\s*([^(),]+?)\\s*\\)$");

    /** Job. */
    public static class Job
            extends Writer.Job
    {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

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
            String authDb = connConf.getString(KeyConstant.MONGO_AUTH_DB, dbName);
            // the credentials live next to the connection, exactly where the task reads them
            String username = originalConfig.getString(USERNAME);
            String password = originalConfig.getString(PASSWORD);
            if (password != null && password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
                // encrypted password, need to decrypt
                password = EncryptUtil.decrypt(password.substring(Constant.ENC_PASSWORD_PREFIX.length(), password.length() - 1));
            }
            this.mongoClient = StringUtils.isEmpty(username) || StringUtils.isEmpty(password)
                    ? MongoUtil.initMongoClient(address)
                    : MongoUtil.initCredentialMongoClient(address, username, password, authDb);

            // preSql is a writer parameter, as in every other writer; the connection block is still
            // read because that is where the plugin used to take it from
            String preSqls = originalConfig.getString(PRE_SQL);
            if (StringUtils.isBlank(preSqls)) {
                preSqls = connConf.getString(PRE_SQL);
            }
            else if (StringUtils.isNotBlank(connConf.getString(PRE_SQL))) {
                LOG.warn("Both [preSql] and [connection.preSql] are configured, [preSql] is used");
            }
            if (StringUtils.isNotBlank(preSqls)) {
                executePreSql(mongoClient, dbName, collection, Configuration.from(preSqls));
            }
        }

        private void executePreSql(MongoClient mongoClient, String database, String collection, Configuration preSql)
        {
            String type = preSql.getString("type");
            if (StringUtils.isBlank(type)) {
                return;
            }

            MongoCollection<Document> col = mongoClient.getDatabase(database).getCollection(collection);
            if (type.equalsIgnoreCase("drop")) {
                col.drop();
            }
            else if (type.equalsIgnoreCase("remove")) {
                col.deleteMany(buildRemoveFilter(preSql));
            }
            else {
                LOG.warn("Unsupported preSql type [{}], only [drop] and [remove] are supported, nothing is done", type);
            }
        }

        /**
         * Build the filter of a preSql remove operation. Only {@code item} parses the conditions one by one,
         * {@code json} is a raw filter document, both may be combined.
         */
        private Document buildRemoveFilter(Configuration preSql)
        {
            Document filter = MongoUtil.parseFilter(preSql.get("json"), "preSql.json");

            List<Configuration> items = preSql.getListConfiguration("item");
            if (!items.isEmpty()) {
                Document conditions = new Document();
                for (Configuration item : items) {
                    String name = item.getString("name");
                    if (StringUtils.isBlank(name)) {
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                "Each preSql remove item requires a [name]");
                    }
                    String condition = item.getString("condition");
                    conditions.put(name, StringUtils.isBlank(condition)
                            ? item.get("value") : new Document(condition, item.get("value")));
                }
                filter = filter == null ? conditions : new Document("$and", List.of(filter, conditions));
            }

            if (filter == null || filter.isEmpty()) {
                // a missing filter would delete every document of the collection
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The preSql remove operation requires a non-empty [json] or [item] condition");
            }
            return filter;
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

    /** Task. */
    public static class Task
            extends Writer.Task
    {

        /** An unordered batch keeps the documents the server accepts, one rejected document stops no other. */
        private static final InsertManyOptions UNORDERED_INSERT = new InsertManyOptions().ordered(false);
        private static final BulkWriteOptions UNORDERED_BULK = new BulkWriteOptions().ordered(false);
        private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);

        private MongoClient mongoClient;

        private String database = null;
        private String collection = null;
        private int batchSize;
        private boolean wildcardMode = false;
        private List<ColumnPlan> columnPlans = null;
        private boolean update = false;
        private boolean merge = false;
        private String updateKey = null;
        private String[] updateKeyPath = null;
        private String opColumn = null;
        private String[] opColumnPath = null;

        /** Buffered documents together with their source records, so a failed write can be reported as a dirty record. */
        private final List<Pending> buffer = new ArrayList<>();

        /** A converted document and the record it was built from. */
        private record Pending(Record record, Document data) {}

        /** A buffered document together with the write its op column asks for. */
        private record MergeEntry(Pending pending, WriteModel<Document> model) {}

        @Override
        public void init()
        {
            Configuration writerSliceConfig = this.getPluginJobConf();
            String userName = writerSliceConfig.getString(USERNAME);
            String password = writerSliceConfig.getString(PASSWORD);
            if (password != null && password.startsWith(Constant.ENC_PASSWORD_PREFIX)) {
                password = EncryptUtil.decrypt(password.substring(Constant.ENC_PASSWORD_PREFIX.length(), password.length() - 1));
            }
            Configuration connConf = writerSliceConfig.getConfiguration(CONNECTION);
            this.database = connConf.getString(DATABASE);
            String authDb = connConf.getString(KeyConstant.MONGO_AUTH_DB, this.database);
            List<Object> addressList = connConf.getList(KeyConstant.MONGO_ADDRESS, Object.class);
            this.mongoClient = StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password) ?
                    MongoUtil.initCredentialMongoClient(addressList, userName, password, authDb) :
                    MongoUtil.initMongoClient(addressList);

            this.collection = connConf.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = writerSliceConfig.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);

            buildColumnPlans(writerSliceConfig);
            parseWriteMode(writerSliceConfig);
            validateKeyConfiguration();
        }

        /**
         * Resolve the destination columns once per task, either as a column plan per column or as
         * wildcard mode where every record carries one whole document as a JSON string.
         */
        private void buildColumnPlans(Configuration writerSliceConfig)
        {
            Object rawColumn = writerSliceConfig.get(COLUMN);
            if (rawColumn == null) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The configuration column is required, configure the column list or [\"*\"]");
            }

            // a single column may be configured either as one object or as a one element list
            List<?> columnConf = rawColumn instanceof List<?> list ? list : List.of(rawColumn);
            if (columnConf.size() == 1 && "*".equals(String.valueOf(columnConf.get(0)))) {
                this.wildcardMode = true;
                return;
            }

            List<ColumnPlan> plans = new ArrayList<>(columnConf.size());
            for (Object columnMeta : columnConf) {
                if (!(columnMeta instanceof Map)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "Each configured column must be an object carrying [name] and [type]");
                }
                plans.add(ColumnPlan.of(Configuration.from(JSON.toJSONString(columnMeta))));
            }
            this.columnPlans = plans;
        }

        private void parseWriteMode(Configuration writerSliceConfig)
        {
            String writeMode = writerSliceConfig.getString(WRITE_MODE, "insert").trim();
            if ("insert".equalsIgnoreCase(writeMode)) {
                return;
            }
            // the mode name is matched case-insensitively, but the whole value has to be the mode:
            // a typo or trailing text must fail the job instead of writing every record as an insert
            String modeName = writeMode.toLowerCase(Locale.ROOT);
            if (modeName.startsWith("update(")) {
                parseUpdateMode(writeMode);
            }
            else if (modeName.startsWith("merge(")) {
                parseMergeMode(writeMode);
            }
            else {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Unsupported writeMode [%s], expected insert, update(field) or merge(key, opColumn)",
                                writeMode));
            }
        }

        /**
         * Parse update(field). The whole value has to match the mode exactly: trailing text, a
         * second field or a parenthesis inside the field name make the configuration fail instead
         * of quietly turning into a key like "id)".
         */
        private void parseUpdateMode(String writeMode)
        {
            Matcher matcher = UPDATE_MODE_PATTERN.matcher(writeMode);
            if (!matcher.matches()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Unsupported writeMode [%s], the update mode must be written as update(field), "
                                + "for example update(unique_id)", writeMode));
            }
            String key = matcher.group(1).trim();
            if (key.isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The update mode takes exactly one non-empty field, for example update(unique_id)");
            }
            this.update = true;
            this.updateKey = key;
            this.updateKeyPath = ColumnPlan.splitPath(this.updateKey);
        }

        /**
         * Parse merge(key, opColumn). Every record names its operation in the op column: A, I, U,
         * INSERT or UPDATE writes the document matched by the key, D or DELETE deletes it, and any
         * other value including null is a dirty record. Both parameters are required so the
         * configuration always says which column drives the operation, and the op column itself is
         * never stored in the collection. The whole value has to match the mode exactly.
         */
        private void parseMergeMode(String writeMode)
        {
            Matcher matcher = MERGE_MODE_PATTERN.matcher(writeMode);
            if (!matcher.matches()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Unsupported writeMode [%s], the merge mode must be written as merge(key, opColumn), "
                                + "for example merge(unique_id, op)", writeMode));
            }
            String key = matcher.group(1).trim();
            String op = matcher.group(2).trim();
            if (key.isEmpty() || op.isEmpty()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The merge mode requires a non-empty merge key and op column, for example merge(unique_id, op)");
            }
            this.merge = true;
            this.updateKey = key;
            this.updateKeyPath = ColumnPlan.splitPath(this.updateKey);
            this.opColumn = op;
            this.opColumnPath = ColumnPlan.splitPath(this.opColumn);
        }

        /**
         * Fail fast when the merge key / update field or the op column is not part of the
         * configured column list: every record would be reported as dirty instead, which hides a
         * plain configuration mistake. A key may name the parent of configured leaf columns. A
         * wildcard record carries its own keys, so only the records themselves can tell.
         */
        private void validateKeyConfiguration()
        {
            if (wildcardMode || (!merge && !update)) {
                return;
            }
            String keyKind = merge ? "merge" : "update";
            boolean keyConfigured = columnPlans.stream()
                    .anyMatch(plan -> plan.name().equals(updateKey) || plan.name().startsWith(updateKey + "."));
            if (!keyConfigured) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The %s key [%s] must be part of the configured column list",
                                keyKind, updateKey));
            }
            if (merge) {
                boolean opConfigured = columnPlans.stream().anyMatch(plan -> plan.name().equals(opColumn));
                if (!opConfigured) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The merge op column [%s] must be part of the configured column list", opColumn));
                }
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            MongoCollection<Document> col = mongoClient.getDatabase(database).getCollection(this.collection, Document.class);
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                Document data = processRecord(record);
                if (data != null) {
                    buffer.add(new Pending(record, data));
                }
                if (buffer.size() >= this.batchSize) {
                    flush(col);
                }
            }
            flush(col);
        }

        /**
         * Write the buffered documents as one unordered batch. The server keeps every document it
         * accepts and names the position of the ones it rejects, so a rejected document is reported
         * by that position and nothing else is touched. Retrying the whole batch instead would
         * report the documents that were written before the rejected one as failures and cost one
         * round trip per document.
         */
        private void flush(MongoCollection<Document> collection)
        {
            if (buffer.isEmpty()) {
                return;
            }
            try {
                if (merge) {
                    flushMerge(collection);
                }
                else if (update) {
                    List<ReplaceOneModel<Document>> models = new ArrayList<>(buffer.size());
                    for (Pending pending : buffer) {
                        models.add(new ReplaceOneModel<>(buildUpdateQuery(pending.data()), pending.data(), UPSERT));
                    }
                    collection.bulkWrite(models, UNORDERED_BULK);
                }
                else {
                    List<Document> dataList = new ArrayList<>(buffer.size());
                    for (Pending pending : buffer) {
                        dataList.add(pending.data());
                    }
                    collection.insertMany(dataList, UNORDERED_INSERT);
                }
            }
            catch (MongoBulkWriteException e) {
                collectRejected(e, buffer);
            }
            catch (MongoException e) {
                // a failure below the write protocol leaves it unknown which documents of the batch
                // arrived, so they are written one at a time to lose as little as possible
                LOG.warn("Failed to write a batch of [{}] documents, try to write them one at a time. reason: {}",
                        buffer.size(), e.getMessage());
                writeOneByOne(collection);
            }
            finally {
                buffer.clear();
            }
        }

        /**
         * Report the documents a batch write rejected as dirty records. Their position within the
         * request is the position in [sources], the driver keeps the index of the original request.
         */
        private void collectRejected(MongoBulkWriteException e, List<Pending> sources)
        {
            List<BulkWriteError> errors = e.getWriteErrors();
            if (!errors.isEmpty()) {
                LOG.warn("MongoDB rejected [{}] of [{}] documents of a batch, they are collected as dirty records",
                        errors.size(), sources.size());
            }
            for (BulkWriteError error : errors) {
                int index = error.getIndex();
                if (index < 0 || index >= sources.size()) {
                    // the driver named a position outside of the batch, the document cannot be identified
                    LOG.error("MongoDB rejected a document at position [{}] of a batch of [{}] documents: {}",
                            index, sources.size(), error.getMessage());
                    continue;
                }
                super.getTaskPluginCollector().collectDirtyRecord(sources.get(index).record(),
                        String.format("MongoDB rejected the document, code [%d]: %s", error.getCode(), error.getMessage()));
            }
            if (e.getWriteConcernError() != null) {
                LOG.error("The write concern was not met for a batch of [{}] documents: {}",
                        sources.size(), e.getWriteConcernError().getMessage());
            }
        }

        /** Write the buffered documents one by one, collecting each document the server rejects. */
        private void writeOneByOne(MongoCollection<Document> collection)
        {
            for (Pending pending : buffer) {
                try {
                    if (update) {
                        collection.replaceOne(buildUpdateQuery(pending.data()), pending.data(), UPSERT);
                    }
                    else {
                        collection.insertOne(pending.data());
                    }
                }
                catch (MongoException ex) {
                    LOG.debug("Failed to write one document: {}", ex.getMessage());
                    super.getTaskPluginCollector().collectDirtyRecord(pending.record(), ex);
                }
            }
        }

        /**
         * Write the buffered documents according to their op column: A, I, U, INSERT or UPDATE
         * replaces the document named by the merge key with an upsert, D or DELETE deletes it, any
         * other value including null is a dirty record. The op column is a control field and never
         * reaches the collection. A key that appears more than once in a batch keeps its last
         * operation, so the final state does not depend on the order an unordered batch happens to
         * run in.
         */
        private void flushMerge(MongoCollection<Document> collection)
        {
            Map<String, MergeEntry> entries = new LinkedHashMap<>(buffer.size());
            int classified = 0;
            for (Pending pending : buffer) {
                Document data = pending.data();
                if (!containsNestedField(data, opColumnPath)) {
                    super.getTaskPluginCollector().collectDirtyRecord(pending.record(),
                            String.format("The merge op column [%s] is missing, the record does not name its operation",
                                    opColumn));
                    continue;
                }
                Object rawOp = removeNestedField(data, opColumnPath);
                String op = rawOp == null ? "" : rawOp.toString().trim();
                if (StringUtils.isBlank(op)) {
                    super.getTaskPluginCollector().collectDirtyRecord(pending.record(),
                            String.format("The merge op column [%s] is empty, it does not name an operation", opColumn));
                    continue;
                }
                boolean delete;
                if ("D".equalsIgnoreCase(op) || "DELETE".equalsIgnoreCase(op)) {
                    delete = true;
                }
                else if ("A".equalsIgnoreCase(op) || "I".equalsIgnoreCase(op) || "U".equalsIgnoreCase(op)
                        || "INSERT".equalsIgnoreCase(op) || "UPDATE".equalsIgnoreCase(op)) {
                    delete = false;
                }
                else {
                    super.getTaskPluginCollector().collectDirtyRecord(pending.record(),
                            String.format("Unknown merge op [%s], only [A], [I], [U], [D], [INSERT], [UPDATE] "
                                    + "and [DELETE] are understood", op));
                    continue;
                }

                Object keyValue = getNestedValue(data, updateKeyPath);
                if (keyValue == null) {
                    // processRecord rejects such a record already, the guard keeps a null key from becoming a {key: null} filter
                    super.getTaskPluginCollector().collectDirtyRecord(pending.record(),
                            String.format("The record carries no value at the merge key [%s]", updateKey));
                    continue;
                }
                // a subdocument filter matches the whole subdocument, the dotted path names the field
                Document query = new Document(String.join(".", updateKeyPath), keyValue);
                WriteModel<Document> model = delete
                        ? new DeleteOneModel<>(query)
                        : new ReplaceOneModel<>(query, data, UPSERT);
                entries.put(foldKey(keyValue), new MergeEntry(pending, model));
                classified++;
            }

            if (entries.size() < classified) {
                // folded duplicates are invisible to the framework statistics, the debug log is the only trace of them
                LOG.debug("folded [{}] of [{}] classified records into their key's last operation; only that operation is written",
                        classified - entries.size(), classified);
            }

            if (entries.isEmpty()) {
                return;
            }
            List<MergeEntry> mergeEntries = new ArrayList<>(entries.values());
            List<WriteModel<Document>> models = mergeEntries.stream().map(MergeEntry::model).toList();
            try {
                collection.bulkWrite(models, UNORDERED_BULK);
            }
            catch (MongoBulkWriteException e) {
                collectRejected(e, mergeEntries.stream().map(MergeEntry::pending).toList());
            }
            catch (MongoException e) {
                LOG.warn("Failed to write a merge batch of [{}] documents, try to write them one at a time. reason: {}",
                        models.size(), e.getMessage());
                writeMergeOneByOne(collection, mergeEntries);
            }
        }

        /** Write the merge documents one by one, collecting each document the server rejects. */
        private void writeMergeOneByOne(MongoCollection<Document> collection, List<MergeEntry> entries)
        {
            for (MergeEntry entry : entries) {
                try {
                    collection.bulkWrite(List.of(entry.model()), UNORDERED_BULK);
                }
                catch (MongoException ex) {
                    LOG.debug("Failed to write one document: {}", ex.getMessage());
                    super.getTaskPluginCollector().collectDirtyRecord(entry.pending().record(), ex);
                }
            }
        }

        private Document buildUpdateQuery(Document data)
        {
            // a subdocument filter matches the whole subdocument, the dotted path names the field
            return new Document(String.join(".", updateKeyPath), getNestedValue(data, updateKeyPath));
        }

        private Document processRecord(Record record)
        {
            try {
                Document data;
                if (this.wildcardMode) {
                    String json = record.getColumn(0).asString();
                    // an empty column means an empty document, which is not worth inserting
                    if (StringUtils.isEmpty(json)) {
                        return null;
                    }
                    // the driver's extended JSON parser restores ObjectId, Date and so on
                    data = Document.parse(json);
                }
                else {
                    data = new Document();
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        processColumn(record.getColumn(i), getColumnPlan(i), data);
                    }
                }
                // a whole document carries its own keys, so the check belongs to both paths
                if ((update || merge) && getNestedValue(data, updateKeyPath) == null) {
                    // the query of such a record would be {key: null}, which matches every document
                    // where the field is missing or null, and the write would overwrite the first of them
                    throw new IllegalArgumentException(String.format(
                            "The record carries no value at the %s key [%s], it does not name the document to write",
                            merge ? "merge" : "update", updateKey));
                }
                return data;
            }
            catch (AddaxException e) {
                // a configuration error would make every record dirty, fail the task instead
                throw e;
            }
            catch (Exception e) {
                super.getTaskPluginCollector().collectDirtyRecord(record, e);
                return null;
            }
        }

        private ColumnPlan getColumnPlan(int index)
        {
            if (index >= columnPlans.size()) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The record carries more columns than the configured column list");
            }
            return columnPlans.get(index);
        }

        private void processColumn(Column column, ColumnPlan plan, Document data)
        {
            Object rawData = column.getRawData();
            if (rawData == null || (rawData instanceof String str && str.isEmpty())) {
                setNestedField(data, plan.path(),
                        plan.type() == ColumnType.ARRAY ? List.of() : column.asString());
                return;
            }

            if (column instanceof StringColumn) {
                processStringColumn(column, plan, data);
            }
            else {
                processPrimitiveColumn(column, plan, data);
            }
        }

        private void processStringColumn(Column column, ColumnPlan plan, Document data)
        {
            String value = column.asString();
            switch (plan.type()) {
                case OBJECT_ID -> setNestedField(data, plan.path(), new ObjectId(value));
                case ARRAY -> {
                    String[] items = plan.splitter().split(value);
                    Function<String, Object> itemParser = plan.itemParser();
                    setNestedField(data, plan.path(), itemParser == null
                            ? List.of(items) : Arrays.stream(items).map(itemParser).toList());
                }
                case JSON -> setNestedField(data, plan.path(), JSON.toJSON(JSON.parse(value)));
                default -> setNestedField(data, plan.path(), value);
            }
        }

        private void processPrimitiveColumn(Column column, ColumnPlan plan, Document data)
        {
            switch (plan.type()) {
                case INT -> setNestedField(data, plan.path(), Math.toIntExact(column.asLong()));
                case LONG -> setNestedField(data, plan.path(), column.asLong());
                case DATE -> setNestedField(data, plan.path(), column.asDate());
                case DOUBLE -> setNestedField(data, plan.path(), column.asDouble());
                case BOOL -> setNestedField(data, plan.path(), column.asBoolean());
                case BYTES -> setNestedField(data, plan.path(), column.asBytes());
                default -> setNestedField(data, plan.path(), column.asString());
            }
        }

        /**
         * Store a value under its destination path, creating the intermediate documents of a
         * dotted path as needed. An existing non document value on the path is replaced.
         */
        private void setNestedField(Document root, String[] path, Object value)
        {
            Document current = root;
            for (int i = 0; i < path.length - 1; i++) {
                if (current.get(path[i]) instanceof Document child) {
                    current = child;
                }
                else {
                    Document child = new Document();
                    current.put(path[i], child);
                    current = child;
                }
            }
            current.put(path[path.length - 1], value);
        }

        private Object getNestedValue(Document root, String[] path)
        {
            Object current = root;
            for (String part : path) {
                if (!(current instanceof Document currentDoc)) {
                    return null;
                }
                current = currentDoc.get(part);
                if (current == null) {
                    return null;
                }
            }
            return current;
        }

        /**
         * The identity of a key in one batch, following MongoDB equality where the numeric types of
         * the same value match each other. The class name still separates values MongoDB compares
         * unequal, such as an ObjectId and the string of its hexadecimal form.
         */
        private String foldKey(Object keyValue)
        {
            if (keyValue instanceof Number number) {
                try {
                    // floating-point values fold by their exact binary value: NumberDecimal("0.1")
                    // and the double 0.1 print the same but MongoDB compares them unequal, while
                    // 1, 1L and 1.0 and 0.5/NumberDecimal("0.5") compare equal and have to fold
                    BigDecimal value;
                    if (keyValue instanceof Decimal128 decimal) {
                        value = decimal.bigDecimalValue();
                    }
                    else if (keyValue instanceof BigDecimal bigDecimal) {
                        value = bigDecimal;
                    }
                    else if (keyValue instanceof Double || keyValue instanceof Float) {
                        value = new BigDecimal(number.doubleValue());
                    }
                    else {
                        value = BigDecimal.valueOf(number.longValue());
                    }
                    return "number:" + value.stripTrailingZeros().toPlainString();
                }
                catch (NumberFormatException | ArithmeticException e) {
                    // NaN and the infinities have no decimal form; they compare equal across
                    // double and decimal in MongoDB, so the printed value folds them together
                    return "number:" + number;
                }
            }
            return keyValue.getClass().getSimpleName() + ":" + keyValue;
        }

        /** Whether the document carries the field of a dotted path, which tells a blank op from a missing op column. */
        private boolean containsNestedField(Document root, String[] path)
        {
            Object current = root;
            for (int i = 0; i < path.length - 1; i++) {
                if (!(current instanceof Document currentDoc)) {
                    return false;
                }
                current = currentDoc.get(path[i]);
            }
            return current instanceof Document currentDoc && currentDoc.containsKey(path[path.length - 1]);
        }

        /** Remove the field of a dotted path, returns its value or null when the path does not lead to a field. */
        private Object removeNestedField(Document root, String[] path)
        {
            if (path.length == 1) {
                return root.remove(path[0]);
            }
            Document current = root;
            for (int i = 0; i < path.length - 1; i++) {
                if (!(current.get(path[i]) instanceof Document child)) {
                    return null;
                }
                current = child;
            }
            return current.remove(path[path.length - 1]);
        }

        @Override
        public void destroy()
        {
            if (mongoClient != null) {
                mongoClient.close();
                mongoClient = null;
            }
        }

        /** The MongoDB type a column is written as, resolved from the configured type name once per task. */
        private enum ColumnType
        {
            OBJECT_ID, ARRAY, JSON, INT, LONG, DATE, DOUBLE, BOOL, BYTES, STRING;

            static ColumnType of(String type)
            {
                return switch (type.toLowerCase(Locale.ROOT)) {
                    case "objectid" -> OBJECT_ID;
                    case "array" -> ARRAY;
                    case "json" -> JSON;
                    case "int", "int32" -> INT;
                    case "long" -> LONG;
                    case "date" -> DATE;
                    case "double" -> DOUBLE;
                    case "bool" -> BOOL;
                    case "bytes" -> BYTES;
                    default -> STRING;
                };
            }
        }

        /** Column metadata resolved once per task, so the per record path parses no names and types. */
        private record ColumnPlan(String name, String[] path, ColumnType type, Pattern splitter,
                                  Function<String, Object> itemParser)
        {
            static ColumnPlan of(Configuration meta)
            {
                String name = meta.getString(KeyConstant.COLUMN_NAME);
                String type = meta.getString(KeyConstant.COLUMN_TYPE);
                if (StringUtils.isBlank(name) || StringUtils.isBlank(type)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The column configuration requires both [name] and [type]");
                }

                ColumnType columnType = ColumnType.of(type);
                if (columnType != ColumnType.ARRAY) {
                    return new ColumnPlan(name, splitPath(name), columnType, null, null);
                }

                String rawSplitter = meta.getString(KeyConstant.COLUMN_SPLITTER);
                if (StringUtils.isEmpty(rawSplitter)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The column [%s] is declared as an array, but no splitter is specified", name));
                }
                // the splitter is a literal separator, not a regular expression
                return new ColumnPlan(name, splitPath(name), columnType,
                        Pattern.compile(Pattern.quote(rawSplitter)),
                        itemParser(meta.getString(KeyConstant.ITEM_TYPE)));
            }

            static String[] splitPath(String name)
            {
                return name.contains(".") ? name.split("\\.") : new String[] {name};
            }

            /** @return the parser of one array item, or null to keep the items as strings */
            private static Function<String, Object> itemParser(String itemType)
            {
                if (StringUtils.isEmpty(itemType)) {
                    return null;
                }
                return switch (itemType.toUpperCase(Locale.ROOT)) {
                    case "DOUBLE" -> Double::parseDouble;
                    case "INT" -> Integer::parseInt;
                    case "LONG" -> Long::parseLong;
                    case "BOOL" -> Boolean::parseBoolean;
                    case "BYTES" -> Byte::parseByte;
                    default -> null;
                };
            }
        }
    }
}
