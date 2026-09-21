/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.s3writer;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.writer.s3writer.writer.OrcWriter;
import com.wgzhao.addax.plugin.writer.s3writer.writer.ParquetWriter;
import com.wgzhao.addax.plugin.writer.s3writer.writer.TextWriter;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.*;

import static com.wgzhao.addax.core.spi.ErrorCode.*;

/** S3 Writer. */
public class S3Writer
        extends Writer
{
    /** Job. */
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        /** One delete request carries this many keys at most, a longer list has to be batched. */
        private static final int MAX_DELETE_KEYS = 1000;

        private Configuration writerSliceConfig = null;
        private S3Client s3Client = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.s3Client = S3Util.initS3Client(this.writerSliceConfig);
        }

        @Override
        public void destroy()
        {
            if (this.s3Client != null) {
                this.s3Client.close();
            }
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(S3Key.REGION, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.ACCESS_ID, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.ACCESS_KEY, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.BUCKET, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.OBJECT, REQUIRED_VALUE);

            StorageWriterUtil.validateParameter(this.writerSliceConfig);
        }

        @Override
        public void prepare()
        {
            LOG.info("begin do prepare...");
            String bucket = this.writerSliceConfig.getString(S3Key.BUCKET);
            String object = this.writerSliceConfig.getString(S3Key.OBJECT);
            String writeMode = this.writerSliceConfig.getString(S3Key.WRITE_MODE, "append");

            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info("It will cleanup all objects which starts with [{}] in  [{}]", object, bucket);
                deleteBucketObjects(bucket, object);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("Begin to check for existing objects that starts with [{}] in bucket [{}]", object, bucket);
                List<S3Object> objs = listObjects(bucket, object);
                if (!objs.isEmpty()) {
                    LOG.error("There have {} objects starts with {} in  bucket {} ", objs.size(), object, bucket);
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Object conflict");
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String object = this.writerSliceConfig.getString(S3Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(S3Key.BUCKET);
            String objectName = object;
            String objectSuffix = "";
            // the suffix is what follows the last dot of the name, so `report.2024.csv` keeps its
            // extension instead of turning into `report_<uuid>.2024`
            int suffixAt = suffixIndex(object);
            if (suffixAt > 0) {
                objectName = object.substring(0, suffixAt);
                objectSuffix = object.substring(suffixAt);
            }
            Set<String> allObjects = new HashSet<>();
            for (S3Object obj : listObjects(bucket, object)) {
                allObjects.add(obj.key());
            }

            String fullObjectName;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle the same object name
                Configuration splitTaskConfig = this.writerSliceConfig.clone();
                do {
                    fullObjectName = String.format("%s_%s%s", objectName,
                            Strings.CI.replace(UUID.randomUUID().toString(), "-", ""),
                            objectSuffix
                    );
                }
                while (allObjects.contains(fullObjectName));
                allObjects.add(fullObjectName);
                splitTaskConfig.set(S3Key.OBJECT, fullObjectName);
                LOG.info("split write object name:[{}]", fullObjectName);

                writerSplitConfigs.add(splitTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

        /**
         * find all objects that start with objectName and return
         *
         * @param bucket the S3 bucket name
         * @param objectName the object prefix will be found
         * @return {@link List}
         */
        private List<S3Object> listObjects(String bucket, String objectName)
        {
            String suffix = null;
            int suffixAt = suffixIndex(objectName);
            if (suffixAt > 0) {
                suffix = objectName.substring(suffixAt);
                objectName = objectName.substring(0, suffixAt);
            }
            ListObjectsV2Request listObjects = ListObjectsV2Request
                    .builder()
                    .bucket(bucket)
                    .prefix(objectName)
                    .build();

            List<S3Object> result = new ArrayList<>();
            // every page: a bucket returns 1000 keys at a time, and a check against the first page
            // alone misses the conflict of the thousandth and first object
            for (ListObjectsV2Response res : s3Client.listObjectsV2Paginator(listObjects)) {
                for (S3Object obj : res.contents()) {
                    if (suffix == null || obj.key().endsWith(suffix)) {
                        result.add(obj);
                    }
                }
            }
            return result;
        }

        /**
         * The position of the dot that separates the suffix of an object name, or -1 when the name
         * has none. A dot in a directory name is not a suffix, and the last dot is the one that
         * counts: S3 keys are flat, but a key written as {@code data/report.2024.csv} is meant to
         * keep the name it was given.
         *
         * @param object the object name
         * @return the position of the dot, -1 when there is no suffix
         */
        private static int suffixIndex(String object)
        {
            int dot = object.lastIndexOf('.');
            return dot > object.lastIndexOf('/') ? dot : -1;
        }

        /**
         * delete all objects that start with objectName in bucket
         *
         * @param bucket the S3 bucket name
         * @param objectName the object prefix will be deleted
         */
        private void deleteBucketObjects(String bucket, String objectName)
        {
            List<S3Object> objects = listObjects(bucket, objectName);
            // a delete request carries at most 1000 keys, a longer list has to be sent in batches
            for (int i = 0; i < objects.size(); i += MAX_DELETE_KEYS) {
                List<ObjectIdentifier> toDelete = new ArrayList<>();
                for (S3Object obj : objects.subList(i, Math.min(i + MAX_DELETE_KEYS, objects.size()))) {
                    toDelete.add(ObjectIdentifier.builder().key(obj.key()).build());
                }
                try {
                    DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(Delete.builder().objects(toDelete).build())
                            .build();
                    s3Client.deleteObjects(dor);
                }
                catch (S3Exception e) {
                    throw AddaxException.asAddaxException(RUNTIME_ERROR, e.getMessage());
                }
            }
        }
    }

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private S3Client s3Client;
        private String bucket;
        private String object;
        private String nullFormat;
        private String encoding;
        private char fieldDelimiter;
        private String dateFormat;
        private List<String> header;
        private int maxFileSize;// MB
        private String fileType;
        private String sslEnabled;

        @Override
        public void init()
        {
            Configuration writerSliceConfig = this.getPluginJobConf();
            this.s3Client = S3Util.initS3Client(writerSliceConfig);
            this.bucket = writerSliceConfig.getString(S3Key.BUCKET);
            this.object = writerSliceConfig.getString(S3Key.OBJECT);
            this.nullFormat = writerSliceConfig.getString(S3Key.NULL_FORMAT, Constant.DEFAULT_NULL_FORMAT);
            this.dateFormat = writerSliceConfig.getString(S3Key.DATE_FORMAT, Constant.DEFAULT_DATE_FORMAT);

            this.encoding = writerSliceConfig.getString(S3Key.ENCODING, Constant.DEFAULT_ENCODING);
            this.fieldDelimiter = writerSliceConfig.getChar(S3Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER);
            this.header = writerSliceConfig.getList(S3Key.HEADER, null, String.class);
            // unit MB
            int DEFAULT_MAX_FILE_SIZE = 10 * 10000;
            this.maxFileSize = writerSliceConfig.getInt(S3Key.MAX_FILE_SIZE, DEFAULT_MAX_FILE_SIZE);

            this.fileType = writerSliceConfig.getString(S3Key.FILE_TYPE, "text");
            this.sslEnabled = writerSliceConfig.getString(S3Key.SSL_ENABLED, "true");
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            if ("text".equals(this.fileType)) {
                TextWriter textWriter = new TextWriter()
                        .setBucket(this.bucket)
                        .setDateFormat(this.dateFormat)
                        .setEncoding(this.encoding)
                        .setHeader(this.header)
                        .setNullFormat(this.nullFormat)
                        .setObject(this.object)
                        .setS3Client(this.s3Client)
                        .setFieldDelimiter(this.fieldDelimiter)
                        .setMaxFileSize(this.maxFileSize);
                textWriter.write(lineReceiver, this.getPluginJobConf(), this.getTaskPluginCollector());
            }
            else if ("orc".equals(this.fileType)) {
                OrcWriter orcWriter = new OrcWriter()
                        .setBucket(this.bucket)
                        .setDateFormat(this.dateFormat)
                        .setEncoding(this.encoding)
                        .setHeader(this.header)
                        .setNullFormat(this.nullFormat)
                        .setObject(this.object)
                        .setS3Client(this.s3Client)
                        .setFieldDelimiter(this.fieldDelimiter)
                        .setSslEnabled(this.sslEnabled);
                orcWriter.init(this.getPluginJobConf());
                orcWriter.write(lineReceiver, this.getPluginJobConf(), this.getTaskPluginCollector());
            }
            else if ("parquet".equals(this.fileType)) {
                ParquetWriter parquetWriter = new ParquetWriter()
                        .setBucket(this.bucket)
                        .setDateFormat(this.dateFormat)
                        .setEncoding(this.encoding)
                        .setHeader(this.header)
                        .setNullFormat(this.nullFormat)
                        .setObject(this.object)
                        .setS3Client(this.s3Client)
                        .setFieldDelimiter(this.fieldDelimiter)
                        .setSslEnabled(this.sslEnabled);
                parquetWriter.init(this.getPluginJobConf());
                parquetWriter.write(lineReceiver, this.getPluginJobConf(), this.getTaskPluginCollector());
            }
        }

        @Override
        public void destroy()
        {
            if (this.s3Client != null) {
                this.s3Client.close();
            }
        }
    }
}
