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
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.ENCODING;
import static com.wgzhao.addax.core.base.Key.INDEX;
import static com.wgzhao.addax.core.base.Key.TYPE;
import static com.wgzhao.addax.core.base.Key.VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

public class HdfsReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerOriginConfig = null;
        private HashSet<String> sourceFiles;
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

        public void validate()
        {
            readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS, CONFIG_ERROR);

            // path check
            String pathInString = readerOriginConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = List.of(pathInString);
            }
            else {
                path = readerOriginConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE, "The item path is required.");
                }
                for (String eachPath : path) {
                    if (!eachPath.startsWith("/")) {
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                "The item path [%s] should be a absolute path.".formatted(eachPath));
                    }
                }
            }

            specifiedFileType = readerOriginConfig.getNecessaryValue(Key.FILE_TYPE, REQUIRED_VALUE).toUpperCase();
            if (!HdfsConstant.SUPPORT_FILE_TYPE.contains(specifiedFileType)) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "The file type only supports " + HdfsConstant.SUPPORT_FILE_TYPE + " but not " + specifiedFileType);
            }

            String encoding = this.readerOriginConfig.getString(ENCODING, "UTF-8");

            try {
                Charsets.toCharset(encoding);
            }
            catch (UnsupportedCharsetException uce) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        "The encoding [" +  encoding + "] is unsupported.", uce);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, "Exception occurred", e);
            }
            //check Kerberos
            boolean haveKerberos = readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, REQUIRED_VALUE);
                readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, REQUIRED_VALUE);
            }

            // validate the Columns
            validateColumns();

            // validate compress
            String compress = readerOriginConfig.getString(Key.COMPRESS, "NONE");
            if ("gzip".equalsIgnoreCase(compress)) {
                // correct to gz
                readerOriginConfig.set(Key.COMPRESS, "gz");
            }
        }

        private void validateColumns()
        {

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig.getListConfiguration(COLUMN);
            if (null != column && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'".equals(column.get(0).toString()))) {
                readerOriginConfig.set(COLUMN, new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, may be dateFormat value
                List<Configuration> columns = readerOriginConfig.getListConfiguration(COLUMN);

                if (null == columns || columns.isEmpty()) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The item columns is required.");
                }

                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(TYPE, REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(INDEX);
                    String columnValue = eachColumnConf.getString(VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                "The index or value must have one, both of them are null.");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(CONFIG_ERROR,
                                "The index and value must have one, can not have both.");
                    }
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.info("prepare(), start to getAllFiles...");
            this.sourceFiles = (HashSet<String>) dfsUtil.getAllFiles(path, specifiedFileType);
            LOG.info("It will reading #{} file(s), including [{}}.", sourceFiles.size(), sourceFiles);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {

            LOG.info("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();
            int splitNumber = sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Can not find any file in path : [" + readerOriginConfig.getString(Key.PATH) + "]");

            }

            List<List<String>> splitSourceFiles = FileHelper.splitSourceFiles(new ArrayList<>(sourceFiles), splitNumber);
            for (List<String> files : splitSourceFiles) {
                Configuration splitConfig = readerOriginConfig.clone();
                splitConfig.set(HdfsConstant.SOURCE_FILES, files);
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
            this.specifiedFileType = taskConfig.getNecessaryValue(Key.FILE_TYPE, REQUIRED_VALUE);
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

                switch (specifiedFileType.toUpperCase()) {
                    case HdfsConstant.TEXT, HdfsConstant.CSV -> {
                        var inputStream = dfsUtil.getInputStream(sourceFile);
                        StorageReaderUtil.readFromStream(inputStream, sourceFile, taskConfig,
                                recordSender, getTaskPluginCollector());
                    }
                    case HdfsConstant.ORC ->
                            dfsUtil.orcFileStartRead(sourceFile, recordSender, getTaskPluginCollector());
                    case HdfsConstant.SEQ ->
                            dfsUtil.sequenceFileStartRead(sourceFile, taskConfig, recordSender, getTaskPluginCollector());
                    case HdfsConstant.RC ->
                            dfsUtil.rcFileStartRead(sourceFile, recordSender, getTaskPluginCollector());
                    case HdfsConstant.PARQUET ->
                            dfsUtil.parquetFileStartRead(sourceFile, recordSender, getTaskPluginCollector());
                    default -> throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            """
                            The specifiedFileType: [%s] is unsupported.
                            HdfsReader only support TEXT, CSV, ORC, SEQUENCE, RC, PARQUET now.
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
