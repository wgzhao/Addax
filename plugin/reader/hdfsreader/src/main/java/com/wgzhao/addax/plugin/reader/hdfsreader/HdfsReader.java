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

package com.wgzhao.addax.plugin.reader.hdfsreader;

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Hdfs Reader. */
public class HdfsReader
        extends Reader
{
    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerOriginConfig = null;
        private Set<String> sourceFiles;
        private String specifiedFileType = null;
        private DFSUtil dfsUtil = null;
        private List<String> path = null;

        @Override
        public void init()
        {

            LOG.info("init() begin...");
            this.readerOriginConfig = getPluginJobConf();
            validate();
            dfsUtil = new DFSUtil(readerOriginConfig);
            LOG.info("init() ok and end...");
        }

        /** Validate. */
        public void validate()
        {
            readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS, CONFIG_ERROR);

            // path check: the configuration may hold one path as a plain string or a list of them.
            // Which one it is has to be read from the value itself: asking for a list while the
            // configuration holds a string threw a ClassCastException, and a single path that
            // happens to end with a closing bracket was sent down that path.
            Object configuredPath = readerOriginConfig.get(Key.PATH);
            if (configuredPath == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The item path is required.");
            }
            path = configuredPath instanceof List<?>
                    ? readerOriginConfig.getList(Key.PATH, String.class)
                    : List.of(readerOriginConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE));
            if (path.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The item path is required.");
            }
            for (String eachPath : path) {
                if (!eachPath.startsWith("/")) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            "The item path [%s] should be a absolute path.".formatted(eachPath));
                }
            }

            specifiedFileType = readerOriginConfig.getNecessaryValue(Key.FILE_TYPE, REQUIRED_VALUE).toUpperCase(Locale.ROOT);
            if (!HdfsConstant.SUPPORT_FILE_TYPE.contains(specifiedFileType)) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "The file type only supports " + HdfsConstant.SUPPORT_FILE_TYPE + " but not " + specifiedFileType);
            }

            // Encoding, fieldDelimiter and column layout are shared with every other file
            // reader; keeping a private copy here let the three validations drift apart.
            StorageReaderUtil.validateParameter(readerOriginConfig);

            //check Kerberos
            boolean haveKerberos = readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, REQUIRED_VALUE);
                readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, REQUIRED_VALUE);
            }

            // validate compress
            String compress = readerOriginConfig.getString(Key.COMPRESS, "NONE");
            if ("gzip".equalsIgnoreCase(compress)) {
                // correct to gz
                readerOriginConfig.set(Key.COMPRESS, "gz");
            }
        }

        @Override
        public void prepare()
        {
            LOG.info("prepare(), start to getAllFiles...");
            this.sourceFiles = dfsUtil.getAllFiles(path, specifiedFileType);
            LOG.info("It will read {} file(s): {}.", sourceFiles.size(), sourceFiles);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {

            LOG.info("split() begin...");
            if (sourceFiles.isEmpty()) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Can not find any file in path : [" + readerOriginConfig.getString(Key.PATH) + "]");
            }

            // one task per file: a file is the smallest unit this reader can divide the work into,
            // the parallelism of the job comes from the number of files rather than from the
            // advice of the engine
            List<Configuration> readerSplitConfigs = new ArrayList<>(sourceFiles.size());
            for (String sourceFile : sourceFiles) {
                Configuration splitConfig = readerOriginConfig.clone();
                splitConfig.set(HdfsConstant.SOURCE_FILES, List.of(sourceFile));
                readerSplitConfigs.add(splitConfig);
            }

            return readerSplitConfigs;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private List<String> sourceFiles;
        private String specifiedFileType;
        private DFSUtil dfsUtil = null;

        @Override
        public void init()
        {

            this.taskConfig = getPluginJobConf();
            this.sourceFiles = taskConfig.getList(HdfsConstant.SOURCE_FILES, String.class);
            this.specifiedFileType = taskConfig.getNecessaryValue(Key.FILE_TYPE, REQUIRED_VALUE).toUpperCase(Locale.ROOT);
            this.dfsUtil = new DFSUtil(taskConfig);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {

            LOG.info("Begin to read files");

            for (var sourceFile : this.sourceFiles) {
                LOG.info("Reading file: {}", sourceFile);

                switch (specifiedFileType) {
                    case HdfsConstant.TEXT, HdfsConstant.CSV -> {
                        var inputStream = dfsUtil.getInputStream(sourceFile);
                        StorageReaderUtil.readFromStream(inputStream, sourceFile, taskConfig,
                                recordSender, getTaskPluginCollector());
                    }
                    case HdfsConstant.ORC ->
                            dfsUtil.orcFileStartRead(sourceFile, recordSender, getTaskPluginCollector());
                    case HdfsConstant.SEQ ->
                            dfsUtil.sequenceFileStartRead(sourceFile, taskConfig, recordSender, getTaskPluginCollector());
                    case HdfsConstant.PARQUET ->
                            dfsUtil.parquetFileStartRead(sourceFile, recordSender, getTaskPluginCollector());
                    default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            """
                            The specifiedFileType: [%s] is unsupported.
                            HdfsReader only support TEXT, CSV, ORC, SEQUENCE, PARQUET now.
                            """.formatted(specifiedFileType));
                }

                if (recordSender != null) {
                    recordSender.flush();
                }
            }

            LOG.info("Reading files finished");
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
