package com.wgzhao.addax.plugin.reader.s3reader;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S3Reader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(S3Reader.Job.class);

        private Configuration readerOriginConfig = null;

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
            String region = this.readerOriginConfig.getString(S3Key.REGION);
            if (StringUtils.isBlank(region)) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.REQUIRED_VALUE, "The item region is required");
            }

            String accessId = this.readerOriginConfig.getString(S3Key.ACCESS_ID);
            if (StringUtils.isBlank(accessId)) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.REQUIRED_VALUE, "The item accessId is required");
            }

            String accessKey = this.readerOriginConfig.getString(S3Key.ACCESS_KEY);
            if (StringUtils.isBlank(accessKey)) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.REQUIRED_VALUE, "The item accesskey is required");
            }

            String bucket = this.readerOriginConfig.getString(S3Key.BUCKET);
            if (StringUtils.isBlank(bucket)) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.REQUIRED_VALUE, "The item bucket is required");
            }

            String object = this.readerOriginConfig.getString(S3Key.OBJECT);
            if (StringUtils.isBlank(object)) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.REQUIRED_VALUE, "The item object is required");
            }

            String encoding = this.readerOriginConfig.getString(S3Key.ENCODING, Constant.DEFAULT_ENCODING);
            try {
                Charsets.toCharset(encoding);
            }
            catch (UnsupportedCharsetException uce) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.ILLEGAL_VALUE,
                        String.format("unsupported encoding : [%s]", encoding), uce);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.ILLEGAL_VALUE,
                        String.format("Runtime Error : %s", e.getMessage()), e);
            }

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig.getListConfiguration(S3Key.COLUMN);
            if (null != column && 1 == column.size() && ("\"*\"".equals(column.get(0).toString())
                    || "'*'".equals(column.get(0).toString()))) {
                readerOriginConfig.set(S3Key.COLUMN, new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, maybe with format string
                List<Configuration> columns = this.readerOriginConfig
                        .getListConfiguration(S3Key.COLUMN);

                if (null == columns || columns.size() == 0) {
                    throw AddaxException.asAddaxException(
                            S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The item column is required");
                }

                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(S3Key.TYPE, S3ReaderErrorCode.REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(S3Key.INDEX);
                    String columnValue = eachColumnConf.getString(S3Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(
                                S3ReaderErrorCode.NO_INDEX_VALUE,
                                "You configured type, also configured index or value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(
                                S3ReaderErrorCode.MIXED_INDEX_VALUE,
                                "You configured both index and value");
                    }
                }
            }
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

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // 将每个单独的 object 作为一个 slice
            List<String> objects = parseOriginObjects(readerOriginConfig.getList(S3Key.OBJECT, String.class));
            if (0 == objects.size()) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
                        String.format(
                                "The object %s in bucket %s is not found",
                                this.readerOriginConfig.get(S3Key.OBJECT),
                                this.readerOriginConfig.get(S3Key.BUCKET)));
            }

            for (String object : objects) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(S3Key.OBJECT, object);
                readerSplitConfigs.add(splitedConfig);
                LOG.info(String.format("S3 object to be read:%s", object));
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private List<String> parseOriginObjects(List<String> originObjects)
        {
            List<String> parsedObjects = new ArrayList<>();

            for (String object : originObjects) {
                int firstMetaChar = Math.max(object.indexOf('*'), object.indexOf('?'));

                if (firstMetaChar != -1) {
                    int lastDirSeparator = object.lastIndexOf(IOUtils.DIR_SEPARATOR, firstMetaChar);
                    String parentDir = object.substring(0, lastDirSeparator + 1);
                    List<String> remoteObjects = getRemoteObjects(parentDir);
                    Pattern pattern = Pattern.compile(object.replace("*", ".*")
                            .replace("?", ".?"));

                    for (String remoteObject : remoteObjects) {
                        if (pattern.matcher(remoteObject).matches()) {
                            parsedObjects.add(remoteObject);
                        }
                    }
                }
                else {
                    parsedObjects.add(object);
                }
            }
            return parsedObjects;
        }

        private List<String> getRemoteObjects(String parentDir)
        {

            List<String> remoteObjects = new ArrayList<>();
            S3Client client = S3Util.initS3Client(readerOriginConfig);

            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(readerOriginConfig.getString(S3Key.BUCKET))
                    .build();
            ListObjectsResponse res = client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
                remoteObjects.add(myValue.key());
            }

            return remoteObjects;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Reader.Task.class);

        private Configuration readerSliceConfig;

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("read start");
            String object = readerSliceConfig.getString(S3Key.OBJECT);
            S3Client client = S3Util.initS3Client(readerSliceConfig);

            GetObjectRequest s3Object = GetObjectRequest.builder()
                    .bucket(readerSliceConfig.getString(S3Key.BUCKET))
                    .key(object)
                    .build();
            try {
                InputStream objectStream = client.getObject(s3Object);
                StorageReaderUtil.readFromStream(objectStream, object,
                        this.readerSliceConfig, recordSender,
                        this.getTaskPluginCollector());
                recordSender.flush();
            }
            catch (NoSuchKeyException e) {
                throw AddaxException.asAddaxException(S3ReaderErrorCode.OBJECT_NOT_EXIST,
                        "The object " + object + " does not exists");
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
