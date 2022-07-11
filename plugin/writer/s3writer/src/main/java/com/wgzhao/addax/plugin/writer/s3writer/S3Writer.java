package com.wgzhao.addax.plugin.writer.s3writer;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

public class S3Writer
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private S3Client s3Client = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.s3Client = S3Util.initS3Client(this.writerSliceConfig);
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(S3Key.REGION, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.ACCESS_ID, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.ACCESS_KEY, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.BUCKET, S3WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(S3Key.OBJECT, S3WriterErrorCode.REQUIRED_VALUE);

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
                LOG.info("Begin check exists objects which starts with [%s] in bucket [{}] or not Bucket [{}] ", bucket, object);
                List<S3Object> objs = listObjects(bucket, object);
                if (!objs.isEmpty()) {
                    LOG.error("There have {} objects starts with {} in  bucket {} ", objs.size(), object, bucket);
                    throw AddaxException.asAddaxException(S3WriterErrorCode.ILLEGAL_VALUE, "Object conflict");
                }
            }
        }

        @Override
        public void post()
        {

        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String object = this.writerSliceConfig.getString(S3Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(S3Key.BUCKET);

            Set<String> allObjects = new HashSet<>();
            for (S3Object obj : listObjects(bucket, object)) {
                allObjects.add(obj.key());
            }


            String objectSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same object name
                Configuration splitedTaskConfig = this.writerSliceConfig.clone();

                String fullObjectName;
                objectSuffix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
                fullObjectName = String.format("%s_%s", object, objectSuffix);
                while (allObjects.contains(fullObjectName)) {
                    objectSuffix = StringUtils.replace(UUID.randomUUID().toString(), "-", "");
                    fullObjectName = String.format("%s_%s", object, objectSuffix);
                }
                allObjects.add(fullObjectName);
                splitedTaskConfig.set(S3Key.OBJECT, fullObjectName);
                LOG.info(String.format("split write object name:[%s]", fullObjectName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

        /**
         * find all objects in given bucket
         *
         * @param bucket the S3 bucket name
         * @return {@link List}
         */
        private List<S3Object> listObjects(String bucket)
        {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucket)
                    .build();

            ListObjectsResponse res = s3Client.listObjects(listObjects);
            return res.contents();
        }

        /**
         * delete all objects which starts with objectName in bucket
         *
         * @param bucket the S3 bucket name
         * @param objectName the object prefix will be deleted
         */
        private void deleteBucketObjects(String bucket, String objectName)
        {
            List<S3Object> objects = listObjects(bucket);
            ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
            for (S3Object obj : objects) {
                if (obj.key().startsWith(objectName)) {
                    LOG.info("delete matched object {}", obj.key());
                    toDelete.add(ObjectIdentifier.builder().key(obj.key()).build());
                }
            }
            if (!toDelete.isEmpty()) {
                try {
                    DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(Delete.builder().objects(toDelete).build())
                            .build();
                    s3Client.deleteObjects(dor);
                }
                catch (S3Exception e) {
                    throw AddaxException.asAddaxException(S3WriterErrorCode.S3_COMM_ERROR, e.getMessage());
                }
            }
        }
    }

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
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            // 设置每块字符串长度
            final int partSize = 1024 * 1024 * 10;
            long numberCacual = (this.maxFileSize * 1024 * 1024L) / partSize;
            final long maxPartNumber = numberCacual >= 1 ? numberCacual : 1;
            //warn: may be StringBuffer->StringBuilder
            Record record;

            LOG.info(String.format("begin do write, each object maxFileSize: [%s]MB...", maxPartNumber * 10));
            // First create a multipart upload and get the upload id
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(object)
                    .build();
            CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
            String uploadId = response.uploadId();
            int currPart = 1;
            List<CompletedPart> completedParts = new ArrayList<>();

            UploadPartRequest uploadPartRequest;
            uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(object)
                    .uploadId(uploadId)
                    .partNumber(currPart).build();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
            Charset charset = Charset.forName(encoding);
            boolean needInit = true;
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    if (needInit && ! header.isEmpty()) {
                        // write header
                        outputStream.write(String.join(String.valueOf(fieldDelimiter), header).getBytes(charset));
                        outputStream.write("\n".getBytes(charset));
                        needInit = false;
                    }
                    outputStream.write(record2String(record).getBytes(charset));
                    outputStream.write("\n".getBytes(charset));

                    if (outputStream.size() > partSize) {
                        String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(outputStream.toByteArray())).eTag();
                        CompletedPart completedPart = CompletedPart.builder().partNumber(currPart).eTag(etag).build();
                        completedParts.add(completedPart);
                        currPart += 1;
                        outputStream.reset();

                        uploadPartRequest = UploadPartRequest.builder()
                                .bucket(bucket)
                                .key(object)
                                .uploadId(uploadId)
                                .partNumber(currPart).build();
                        needInit = true;
                    }
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(S3WriterErrorCode.S3_COMM_ERROR, e.getMessage());
                }
            }
            // remain bytes
            if (outputStream.size() > 0) {
                String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(outputStream.toByteArray())).eTag();
                CompletedPart completedPart = CompletedPart.builder().partNumber(currPart).eTag(etag).build();
                completedParts.add(completedPart);
                outputStream.reset();
            }
            // Finally, call completeMultipartUpload operation to tell S3 to merge all uploaded
            // parts and finish the multipart operation.
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(object)
                            .uploadId(uploadId)
                            .multipartUpload(completedMultipartUpload)
                            .build();

            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
            LOG.info("end do write");
        }

        private String record2String(Record record) {
            StringJoiner sj = new StringJoiner(this.fieldDelimiter + "");
            int columnNum = record.getColumnNumber();
            for (int i=0; i < columnNum; i++) {
                Column column = record.getColumn(i);
                if (column == null || column.asString() == null) {
                    sj.add(this.nullFormat);
                }
                assert column != null;
                Column.Type type = column.getType();
                if (type == Column.Type.DATE) {
                    SimpleDateFormat sdf = new SimpleDateFormat(this.dateFormat);
                    sj.add(sdf.format(column.asDate()));
                } else {
                    sj.add(column.asString());
                }
            }
            return sj.toString();
        }

        @Override
        public void prepare()
        {

        }

        @Override
        public void post()
        {

        }

        @Override
        public void destroy()
        {

        }
    }
}
