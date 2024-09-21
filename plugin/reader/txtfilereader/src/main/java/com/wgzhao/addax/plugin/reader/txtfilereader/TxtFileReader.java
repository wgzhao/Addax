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

package com.wgzhao.addax.plugin.reader.txtfilereader;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class TxtFileReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;
        private List<String> sourceFiles;
        private boolean needReadColumnName = false;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            StorageReaderUtil.validateParameter(this.originConfig);
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            if (StringUtils.isBlank(pathInString)) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "the path is required");
            }
            List<String> path;
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE, "the path is required");
                }
            }

            this.sourceFiles = FileHelper.buildSourceTargets(path);

            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);
            if (null != columns && !columns.isEmpty()) {
                for (Configuration eachColumnConf : columns) {
                    if (null != eachColumnConf.getString(Key.NAME)) {
                        needReadColumnName = true;
                    }
                }
            }
            if (needReadColumnName) {
                convertColumnNameToIndex(this.sourceFiles.get(0));
            }
            LOG.info("The number of files to read is: [{}]", this.sourceFiles.size());
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

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(
                        CONFIG_ERROR, String
                                .format("未能找到待读取的文件,请确认您的配置项path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitSourceFiles = FileHelper.splitSourceFiles(this.sourceFiles, splitNumber);
            for (List<String> files : splitSourceFiles) {
                Configuration splitConfig = this.originConfig.clone();
                splitConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private int getIndexByName(String name, String[] allNames)
        {
            for (int i = 0; i < allNames.length; i++) {
                if (allNames[i].equalsIgnoreCase(name)) {
                    return i;
                }
            }
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE,
                    "The name '" + name + "' DOES NOT exists in file header: " + Arrays.toString(allNames)
            );
        }

        private void convertColumnNameToIndex(String fileName)
        {
            String encoding = this.originConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            int bufferSize = this.originConfig.getInt(Key.BUFFER_SIZE, Constant.DEFAULT_BUFFER_SIZE);
            String delimiter = this.originConfig.getString(Key.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER + "");
            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);
            BufferedReader reader = FileHelper.readCompressFile(fileName, encoding, bufferSize);
            try {
                String fetchLine = reader.readLine();
                String[] columnNames = fetchLine.split(delimiter);
                int index;
                for (Configuration column : columns) {
                    if (column.getString(Key.NAME) != null) {
                        index = getIndexByName(column.getString(Key.NAME), columnNames);
                        column.set(Key.INDEX, index);
                    }
                }
                this.originConfig.set(Key.COLUMN, columns);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                IOUtils.closeQuietly(reader, null);
            }
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = this.readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
        }

        @Override
        public void prepare()
        {
            //
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

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("start read source files...");
            FileInputStream inputStream;
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);
                try {
                    inputStream = new FileInputStream(fileName);
                }
                catch (FileNotFoundException e) {
                    throw AddaxException.asAddaxException(
                            IO_ERROR,
                            "Open file '" + fileName + "' failure"
                    );
                }
                StorageReaderUtil.readFromStream(inputStream, fileName, readerSliceConfig, recordSender, getTaskPluginCollector());
            }
            LOG.debug("end read source files...");
        }
    }
}
