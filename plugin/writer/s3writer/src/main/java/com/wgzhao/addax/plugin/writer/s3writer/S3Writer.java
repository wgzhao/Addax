package com.wgzhao.addax.plugin.writer.s3writer;

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.writer.s3writer.formatwriter.OrcWriter;
import com.wgzhao.addax.plugin.writer.s3writer.formatwriter.ParquetWriter;
import com.wgzhao.addax.plugin.writer.s3writer.formatwriter.TextWriter;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.*;

import static com.wgzhao.addax.core.spi.ErrorCode.*;

public class S3Writer
        extends Writer {
    public static class Job
            extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private S3Client s3Client = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.s3Client = S3Util.initS3Client(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            if (this.s3Client != null) {
                this.s3Client.close();
            }
        }

        private void validateParameter() {
            this.writerSliceConfig.getNecessaryValue(S3Key.REGION, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.ACCESS_ID, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.ACCESS_KEY, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.BUCKET, REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.OBJECT, REQUIRED_VALUE);

            StorageWriterUtil.validateParameter(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            String bucket = this.writerSliceConfig.getString(S3Key.BUCKET);
            String object = this.writerSliceConfig.getString(S3Key.OBJECT);
            String writeMode = this.writerSliceConfig.getString(S3Key.WRITE_MODE, "append");

            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info("It will cleanup all objects which starts with [{}] in  [{}]", object, bucket);
                deleteBucketObjects(bucket, object);
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info("Begin to check for existing objects that starts with [{}] in bucket [{}]", object, bucket);
                List<S3Object> objs = listObjects(bucket, object);
                if (!objs.isEmpty()) {
                    LOG.error("There have {} objects starts with {} in  bucket {} ", objs.size(), object, bucket);
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Object conflict");
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String object = this.writerSliceConfig.getString(S3Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(S3Key.BUCKET);
            String objectName = object;
            String objectSuffix = null;
            // if the object has suffix, it should separate the object name
            if (object.contains(".")) {
                objectName = object.split("\\.", -1)[0];
                objectSuffix = "." + object.split("\\.", -1)[1];
            }
            Set<String> allObjects = new HashSet<>();
            for (S3Object obj : listObjects(bucket, object)) {
                allObjects.add(obj.key());
            }

            String fullObjectName;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same object name
                Configuration splitTaskConfig = this.writerSliceConfig.clone();
                do {
                    fullObjectName = String.format("%s_%s%s", objectName,
                            StringUtils.replace(UUID.randomUUID().toString(), "-", ""),
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
         * find all objects which starts with objectName and return
         *
         * @param bucket     the S3 bucket name
         * @param objectName the object prefix will be found
         * @return {@link List}
         */
        private List<S3Object> listObjects(String bucket, String objectName) {
            String suffix = null;
            if (objectName.contains(".")) {
                suffix = "." + objectName.split("\\.", -1)[1];
                objectName = objectName.split("\\.", -1)[0];
            }
            ListObjectsV2Request listObjects = ListObjectsV2Request
                    .builder()
                    .bucket(bucket)
                    .prefix(objectName)
                    .build();

            ListObjectsV2Response res = s3Client.listObjectsV2(listObjects);

            List<S3Object> objects = res.contents();
            List<S3Object> result = new ArrayList<>();
            for (S3Object obj : objects) {
                if (suffix == null) {
                    result.add(obj);
                } else if (obj.key().endsWith(suffix)) {
                    result.add(obj);
                }
            }
            return result;
        }

        /**
         * delete all objects which starts with objectName in bucket
         *
         * @param bucket     the S3 bucket name
         * @param objectName the object prefix will be deleted
         */
        private void deleteBucketObjects(String bucket, String objectName) {
            List<S3Object> objects = listObjects(bucket, objectName);
            ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
            if (!objects.isEmpty()) {
                for (S3Object obj : objects) {
                    toDelete.add(ObjectIdentifier.builder().key(obj.key()).build());
                }
                try {
                    DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(Delete.builder().objects(toDelete).build())
                            .build();
                    s3Client.deleteObjects(dor);
                } catch (S3Exception e) {
                    throw AddaxException.asAddaxException(RUNTIME_ERROR, e.getMessage());
                }
            }
        }
    }

    public static class Task
            extends Writer.Task {
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
        public void init() {
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
        public void startWrite(RecordReceiver lineReceiver) {
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
            } else if ("orc".equals(this.fileType)) {
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
            } else if ("parquet".equals(this.fileType)) {
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
        public void destroy() {
            if (this.s3Client != null) {
                this.s3Client.close();
            }
        }
    }
}
