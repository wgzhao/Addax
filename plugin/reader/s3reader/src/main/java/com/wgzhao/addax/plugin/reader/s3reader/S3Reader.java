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

package com.wgzhao.addax.plugin.reader.s3reader;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import com.wgzhao.addax.storage.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/** S3 Reader. */
public class S3Reader
        extends Reader
{
    /** Job. */
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

            // Every reader of this kind hands its input to the CSV parser. The key belongs to the
            // writer side, so a job that sets it to json or parquet would otherwise have its data
            // parsed as CSV without a word about it.
            String fileFormat = readerOriginConfig.getString(S3Key.FILE_FORMAT, null);
            if (fileFormat != null && !"csv".equalsIgnoreCase(fileFormat) && !"text".equalsIgnoreCase(fileFormat)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The file format [%s] is not supported, this reader reads csv and text", fileFormat));
            }

            // Encoding, fieldDelimiter and column layout are shared with every other file
            // reader; keeping a private copy here let the three validations drift apart.
            StorageReaderUtil.validateParameter(readerOriginConfig);

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

            List<String> objects = parseOriginObjects(readerOriginConfig.getList(S3Key.OBJECT, String.class));
            if (objects.isEmpty()) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR,
                        String.format(
                                "The object %s in bucket %s is not found",
                                this.readerOriginConfig.get(S3Key.OBJECT),
                                this.readerOriginConfig.get(S3Key.BUCKET)));
            }

            // one task per object would build a client per object; grouping them the way the file
            // readers group their files keeps one client per task and leaves the parallelism to
            // the channel number
            int splitNumber = Math.min(objects.size(), Math.max(adviceNumber, 1));
            for (List<String> group : FileHelper.splitSourceFiles(objects, splitNumber)) {
                Configuration splitConfig = this.readerOriginConfig.clone();
                splitConfig.set(S3Key.OBJECT, group);
                readerSplitConfigs.add(splitConfig);
                LOG.info("The objects to be read in one task: {}", group);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        /** The objects to read, with every pattern replaced by the keys it matches. */
        private List<String> parseOriginObjects(List<String> originObjects)
        {
            return originObjects.stream()
                    .flatMap(object -> isPattern(object)
                            ? listObjectsWithPattern(object).stream()
                            : Stream.of(object))
                    // an exact name next to a pattern that covers it names the same object twice,
                    // and reading it twice would duplicate its records in the result
                    .distinct()
                    .sorted()
                    .toList();
        }

        private static boolean isPattern(String object)
        {
            return object.indexOf('*') > -1 || object.indexOf('?') > -1;
        }

        private List<String> listObjectsWithPattern(String pattern)
        {
            // S3 lists by a literal prefix only, the part before the first wildcard narrows the
            // listing down
            String prefix = pattern.substring(0, firstWildcard(pattern));
            Pattern compiledPattern = toRegex(pattern);

            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .build();

            List<String> remoteObjects = new ArrayList<>();
            // the paginator walks every page, a bucket returns 1000 keys at a time
            for (ListObjectsV2Response response : client.listObjectsV2Paginator(listObjectsV2Request)) {
                for (S3Object s3Object : response.contents()) {
                    if (compiledPattern.matcher(s3Object.key()).matches()) {
                        remoteObjects.add(s3Object.key());
                    }
                }
            }

            return remoteObjects;
        }

        /** The position of the first wildcard, the length of the pattern when it has none. */
        private static int firstWildcard(String pattern)
        {
            for (int i = 0; i < pattern.length(); i++) {
                char ch = pattern.charAt(i);
                if (ch == '*' || ch == '?') {
                    return i;
                }
            }
            return pattern.length();
        }

        /**
         * The regular expression matching the keys of a pattern. Only {@code *} and {@code ?} are
         * wildcards, everything else is a literal part of an object key: a dot, a plus sign or a
         * bracket in a name must not be read as a regular expression, and a bracket that never
         * closes would not even compile.
         *
         * @param pattern the object pattern
         * @return the expression the keys are matched against
         */
        private static Pattern toRegex(String pattern)
        {
            StringBuilder regex = new StringBuilder(pattern.length() * 2);
            int literalStart = 0;
            for (int i = 0; i < pattern.length(); i++) {
                char ch = pattern.charAt(i);
                if (ch != '*' && ch != '?') {
                    continue;
                }
                regex.append(Pattern.quote(pattern.substring(literalStart, i)));
                regex.append(ch == '*' ? ".*" : ".");
                literalStart = i + 1;
            }
            return Pattern.compile(regex.append(Pattern.quote(pattern.substring(literalStart))).toString());
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("Begin to start reading");
            List<String> objects = readerSliceConfig.getList(S3Key.OBJECT, String.class);
            String bucketName = readerSliceConfig.getString(S3Key.BUCKET);
            // one client for every object of the task: a client per object would repeat the
            // connection pool and the TLS handshake for each of them
            try (S3Client client = S3Util.initS3Client(readerSliceConfig)) {
                for (String object : objects) {
                    LOG.info("Begin to read object {}", object);
                    GetObjectRequest request = GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(object)
                            .build();
                    try (InputStream objectStream = client.getObject(request)) {
                        StorageReaderUtil.readFromStream(objectStream, object,
                                this.readerSliceConfig, recordSender,
                                this.getTaskPluginCollector());
                    }
                    catch (NoSuchKeyException e) {
                        // the object was listed for this task and is gone now, an incomplete result
                        // is worse than a job that says so
                        throw AddaxException.asAddaxException(CONFIG_ERROR,
                                String.format("The object '%s' does not exist in bucket '%s'", object, bucketName));
                    }
                    catch (IOException e) {
                        throw AddaxException.asAddaxException(IO_ERROR,
                                String.format("Failed to read the object '%s': %s", object, e.getMessage()), e);
                    }
                }
            }
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
