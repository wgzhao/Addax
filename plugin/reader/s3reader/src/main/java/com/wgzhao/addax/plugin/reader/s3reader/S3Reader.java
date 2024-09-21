package com.wgzhao.addax.plugin.reader.s3reader;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class S3Reader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(S3Reader.Job.class);

        private Configuration readerOriginConfig = null;

        private String bucket;
        private S3Client client = null;

        @Override
        public void init()
        {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.validate();
            LOG.debug("init() ok and end...");
        }

        private void validate()
        {
            readerOriginConfig.getNecessaryValue(S3Key.REGION, REQUIRED_VALUE);
            readerOriginConfig.getNecessaryValue(S3Key.ACCESS_ID, REQUIRED_VALUE);
            readerOriginConfig.getNecessaryValue(S3Key.ACCESS_KEY, REQUIRED_VALUE);
            this.bucket = readerOriginConfig.getNecessaryValue(S3Key.BUCKET, REQUIRED_VALUE);
            readerOriginConfig.getNecessaryValue(S3Key.OBJECT, REQUIRED_VALUE);

            String encoding = readerOriginConfig.getString(S3Key.ENCODING, Constant.DEFAULT_ENCODING);
            try {
                Charsets.toCharset(encoding);
            }
            catch (UnsupportedCharsetException uce) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("unsupported encoding : [%s]", encoding), uce);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Runtime Error : %s", e.getMessage()), e);
            }

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = readerOriginConfig.getListConfiguration(S3Key.COLUMN);
            if (null != column && 1 == column.size() && ("\"*\"".equals(column.get(0).toString())
                    || "'*'".equals(column.get(0).toString()))) {
                readerOriginConfig.set(S3Key.COLUMN, new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, maybe with format string
                List<Configuration> columns = readerOriginConfig.getListConfiguration(S3Key.COLUMN);

                if (null == columns || columns.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            REQUIRED_VALUE,
                            "The item column is required");
                }

                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(S3Key.TYPE, REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(S3Key.INDEX);
                    String columnValue = eachColumnConf.getString(S3Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                "You configured type, also configured index or value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                "You configured both index and value");
                    }
                }
            }
            this.client = S3Util.initS3Client(readerOriginConfig);
        }

        @Override
        public void destroy()
        {
            if (null != this.client) {
                this.client.close();
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // 将每个单独的 object 作为一个 slice
            List<String> objects = parseOriginObjects(readerOriginConfig.getList(S3Key.OBJECT, String.class));
            if (objects.isEmpty()) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR,
                        String.format(
                                "The object %s in bucket %s is not found",
                                this.readerOriginConfig.get(S3Key.OBJECT),
                                this.readerOriginConfig.get(S3Key.BUCKET)));
            }

            for (String object : objects) {
                Configuration splitConfig = this.readerOriginConfig.clone();
                splitConfig.set(S3Key.OBJECT, object);
                readerSplitConfigs.add(splitConfig);
                LOG.info("S3 object to be read {}", object);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private List<String> parseOriginObjects(List<String> originObjects)
        {
            List<String> parsedObjects = new ArrayList<>();
            for (String object : originObjects) {
                if (object.indexOf('*') > -1 || object.indexOf('?') > -1) {
                    List<String> remoteObjects = listObjectsWithPattern(object);
                    parsedObjects.addAll(remoteObjects);
                }
                else {
                    parsedObjects.add(object);
                }
            }
            return parsedObjects;
        }

        private List<String> listObjectsWithPattern(String pattern)
        {
            // Extract the prefix from the pattern up to the first wildcard character
            int firstWildcardIndex = Math.min(
                    pattern.indexOf('*') == -1 ? pattern.length() : pattern.indexOf('*'),
                    pattern.indexOf('?') == -1 ? pattern.length() : pattern.indexOf('?')
            );
            String prefix = pattern.substring(0, firstWildcardIndex);
            // Convert the pattern to a regex
            String regex = pattern.replace("?", ".{1}").replace("*", ".*");
            Pattern compiledPattern = Pattern.compile(regex);

            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response listObjectsV2Response;
            List<String> remoteObjects = new ArrayList<>();
            do {
                listObjectsV2Response = client.listObjectsV2(listObjectsV2Request);

                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    if (compiledPattern.matcher(s3Object.key()).matches()) {
                        remoteObjects.add(s3Object.key());
                    }
                }

                listObjectsV2Request = listObjectsV2Request.toBuilder()
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();
            }
            while (listObjectsV2Response.isTruncated());

            return remoteObjects;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("Begin to start reading");
            String object = readerSliceConfig.getString(S3Key.OBJECT);

            GetObjectRequest s3Object = GetObjectRequest.builder()
                    .bucket(readerSliceConfig.getString(S3Key.BUCKET))
                    .key(object)
                    .build();
            try (S3Client client = S3Util.initS3Client(readerSliceConfig)) {
                InputStream objectStream = client.getObject(s3Object);
                StorageReaderUtil.readFromStream(objectStream, object,
                        this.readerSliceConfig, recordSender,
                        this.getTaskPluginCollector());
                recordSender.flush();
            }
            catch (NoSuchKeyException e) {
                LOG.warn("The object {} does not exists", object);
            }
        }

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
        }

        @Override
        public void destroy()
        {

        }
    }
}
