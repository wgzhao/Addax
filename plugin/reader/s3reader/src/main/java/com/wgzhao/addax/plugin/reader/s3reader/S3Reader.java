package com.wgzhao.addax.plugin.reader.s3reader;

import com.google.common.collect.Sets;
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
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 region");
            }

            String accessId = this.readerOriginConfig.getString(S3Key.ACCESSID);
            if (StringUtils.isBlank(accessId)) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 accessId");
            }

            String accessKey = this.readerOriginConfig.getString(S3Key.ACCESSKEY);
            if (StringUtils.isBlank(accessKey)) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 accessKey");
            }

            String bucket = this.readerOriginConfig.getString(S3Key.BUCKET);
            if (StringUtils.isBlank(bucket)) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 bucket");
            }

            String object = this.readerOriginConfig.getString(S3Key.OBJECT);
            if (StringUtils.isBlank(object)) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 object");
            }

            String fieldDelimiter = this.readerOriginConfig
                    .getString(S3Key.FIELD_DELIMITER);
            // warn: need length 1
            if (null == fieldDelimiter || fieldDelimiter.length() == 0) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "您需要指定 fieldDelimiter");
            }

            String encoding = this.readerOriginConfig
                    .getString(S3Key.ENCODING, Constant.DEFAULT_ENCODING);
            try {
                Charsets.toCharset(encoding);
            }
            catch (UnsupportedCharsetException uce) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.ILLEGAL_VALUE,
                        String.format("不支持的编码格式 : [%s]", encoding), uce);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.ILLEGAL_VALUE,
                        String.format("运行配置异常 : %s", e.getMessage()), e);
            }

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig
                    .getListConfiguration(S3Key.COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                    .equals(column.get(0).toString()))) {
                readerOriginConfig
                        .set(S3Key.COLUMN,
                                new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, may
                // have
                // format
                List<Configuration> columns = this.readerOriginConfig
                        .getListConfiguration(S3Key.COLUMN);

                if (null == columns || columns.size() == 0) {
                    throw AddaxException.asAddaxException(
                            S3ReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                if (null != columns && columns.size() != 0) {
                    for (Configuration eachColumnConf : columns) {
                        eachColumnConf
                                .getNecessaryValue(
                                        S3Key.TYPE,
                                        S3ReaderErrorCode.REQUIRED_VALUE);
                        Integer columnIndex = eachColumnConf
                                .getInt(S3Key.INDEX);
                        String columnValue = eachColumnConf
                                .getString(S3Key.VALUE);

                        if (null == columnIndex && null == columnValue) {
                            throw AddaxException.asAddaxException(
                                    S3ReaderErrorCode.NO_INDEX_VALUE,
                                    "由于您配置了type, 则至少需要配置 index 或 value");
                        }

                        if (null != columnIndex && null != columnValue) {
                            throw AddaxException.asAddaxException(
                                    S3ReaderErrorCode.MIXED_INDEX_VALUE,
                                    "您混合配置了index, value, 每一列同时仅能选择其中一种");
                        }
                    }
                }
            }

            // only support compress: gzip,bzip2,zip
            String compress = this.readerOriginConfig
                    .getString(S3Key.COMPRESS);
            if (StringUtils.isBlank(compress)) {
                this.readerOriginConfig
                        .set(S3Key.COMPRESS,
                                null);
            }
            else {
                Set<String> supportedCompress = Sets
                        .newHashSet("gzip", "bzip2", "zip");
                compress = compress.toLowerCase().trim();
                if (!supportedCompress.contains(compress)) {
                    throw AddaxException
                            .asAddaxException(
                                    S3ReaderErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "仅支持 gzip, bzip2, zip 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
                                            compress));
                }
                this.readerOriginConfig
                        .set(S3Key.COMPRESS,
                                compress);
            }
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare()");
        }

        @Override
        public void post()
        {
            LOG.debug("post()");
        }

        @Override
        public void destroy()
        {
            LOG.debug("destroy()");
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // 将每个单独的 object 作为一个 slice
            List<String> objects = parseOriginObjects(readerOriginConfig
                    .getList(S3Key.OBJECT, String.class));
            if (0 == objects.size()) {
                throw AddaxException.asAddaxException(
                        S3ReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
                        String.format(
                                "未能找到待读取的Object,请确认您的配置项bucket: %s object: %s",
                                this.readerOriginConfig.get(S3Key.BUCKET),
                                this.readerOriginConfig.get(S3Key.OBJECT)));
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
            List<String> parsedObjects = new ArrayList<String>();

            for (String object : originObjects) {
                int firstMetaChar = (object.indexOf('*') > object.indexOf('?')) ? object
                        .indexOf('*') : object.indexOf('?');

                if (firstMetaChar != -1) {
                    int lastDirSeparator = object.lastIndexOf(
                            IOUtils.DIR_SEPARATOR, firstMetaChar);
                    String parentDir = object
                            .substring(0, lastDirSeparator + 1);
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

            LOG.debug(String.format("父文件夹 : %s", parentDir));
            List<String> remoteObjects = new ArrayList<String>();
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

        public static class Task
                extends Reader.Task
        {
            private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);

            private Configuration readerSliceConfig;

            @Override
            public void startRead(RecordSender recordSender)
            {
                LOG.debug("read start");
                String object = readerSliceConfig.getString(S3Key.OBJECT);
                S3Client client = S3Util.initS3Client(readerSliceConfig);

                GetObjectRequest s3Object = GetObjectRequest.builder()
                        .bucket(readerSliceConfig.getString(S3Key.BUCKET))
                        .key(readerSliceConfig.getString(S3Key.ACCESSKEY))
                        .build();

                InputStream objectStream = client.getObject(s3Object);
                StorageReaderUtil.readFromStream(objectStream, object,
                        this.readerSliceConfig, recordSender,
                        this.getTaskPluginCollector());
                recordSender.flush();
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
}
