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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
        private final List<String> path = null;
        private List<String> sourceFiles;
        private Map<String, Pattern> pattern;
        private Map<String, Boolean> isRegexPath;
        private boolean needReadColumnName = false;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            this.pattern = new HashMap<>();
            this.isRegexPath = new HashMap<>();
            StorageReaderUtil.validateParameter(this.originConfig);
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");
            // warn:make sure this regex string
            // warn:no need trim
            for (String eachPath : this.path) {
                String regexString = eachPath.replace("*", ".*").replace("?",".?");
                Pattern pattern = Pattern.compile(regexString);
                this.pattern.put(eachPath, pattern);
            }
            this.sourceFiles = this.buildSourceTargets();
            List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);
            if (null != columns && ! columns.isEmpty()) {
                for(Configuration eachColumnConf : columns) {
                    if (null != eachColumnConf.getString(Key.NAME)) {
                        needReadColumnName = true;
                    }
                }
            }
            if (needReadColumnName) {
                convertColumnNameToIndex(this.sourceFiles.get(0));
            }
            LOG.info("您即将读取的文件数为: [{}]", this.sourceFiles.size());
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
                        TxtFileReaderErrorCode.EMPTY_DIR_EXCEPTION, String
                                .format("未能找到待读取的文件,请确认您的配置项path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    this.sourceFiles, splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        // validate the path, path must be a absolute path
        private List<String> buildSourceTargets()
        {
            // for each path
            Set<String> toBeReadFiles = new HashSet<>();
            for (String eachPath : this.path) {
                int endMark;
                for (endMark = 0; endMark < eachPath.length(); endMark++) {
                    if ('*' == eachPath.charAt(endMark) || '?' == eachPath.charAt(endMark)) {
                        this.isRegexPath.put(eachPath, true);
                        break;
                    }
                }

                String parentDirectory;
                if (BooleanUtils.isTrue(this.isRegexPath.get(eachPath))) {
                    int lastDirSeparator = eachPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
                    parentDirectory = eachPath.substring(0, lastDirSeparator + 1);
                }
                else {
                    this.isRegexPath.put(eachPath, false);
                    parentDirectory = eachPath;
                }
                this.buildSourceTargetsEachPath(eachPath, parentDirectory,
                        toBeReadFiles);
            }
            return Arrays.asList(toBeReadFiles.toArray(new String[0]));
        }

        private void buildSourceTargetsEachPath(String regexPath,
                String parentDirectory, Set<String> toBeReadFiles)
        {
            // 检测目录是否存在，错误情况更明确
            try {
                File dir = new File(parentDirectory);
                boolean isExists = dir.exists();
                if (!isExists) {
                    String message = String.format("您设定的目录不存在 : [%s]", parentDirectory);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(
                            TxtFileReaderErrorCode.FILE_NOT_EXISTS, message);
                }
            }
            catch (SecurityException se) {
                String message = String.format("您没有权限查看目录 : [%s]", parentDirectory);
                LOG.error(message);
                throw AddaxException.asAddaxException(
                        TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
            }

            directoryRover(regexPath, parentDirectory, toBeReadFiles);
        }

        private void directoryRover(String regexPath, String parentDirectory,
                Set<String> toBeReadFiles)
        {
            File directory = new File(parentDirectory);
            // is a normal file
            if (!directory.isDirectory()) {
                if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
                    toBeReadFiles.add(parentDirectory);
                    LOG.info("add file [{}] as a candidate to be read.", parentDirectory);
                }
            }
            else {
                // 是目录
                try {
                    // warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
                    File[] files = directory.listFiles();
                    if (null != files) {
                        for (File subFileNames : files) {
                            directoryRover(regexPath, subFileNames.getAbsolutePath(), toBeReadFiles);
                        }
                    }
                    else {
                        // warn: 对于没有权限的文件，是直接throw AddaxException
                        String message = String.format("您没有权限查看目录 : [%s]", directory);
                        LOG.error(message);
                        throw AddaxException.asAddaxException(TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
                    }
                }
                catch (SecurityException e) {
                    String message = String.format("您没有权限查看目录 : [%s]", directory);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(TxtFileReaderErrorCode.SECURITY_NOT_ENOUGH, message, e);
                }
            }
        }

        // 正则过滤
        private boolean isTargetFile(String regexPath, String absoluteFilePath)
        {
            if (this.isRegexPath.get(regexPath)) {
                return this.pattern.get(regexPath).matcher(absoluteFilePath).matches();
            }
            else {
                return true;
            }
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
                int adviceNumber)
        {
            List<List<T>> splitedList = new ArrayList<>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }

        private int getIndexByName(String name, String[] allNames) {
            for (int i=0; i< allNames.length; i++) {
                if (allNames[i].equalsIgnoreCase(name)) {
                    return i;
                }
            }
            throw AddaxException.asAddaxException(
                    TxtFileReaderErrorCode.ILLEGAL_VALUE,
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
                    if (column.getString(Key.NAME) != null ) {
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
        private String encoding;
        private int bufferSize;


        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = this.readerSliceConfig.getList(Key.SOURCE_FILES, String.class);

            this.encoding = this.readerSliceConfig.getString(Key.ENCODING);
            this.bufferSize = readerSliceConfig.getInt(Key.BUFFER_SIZE, Constant.DEFAULT_BUFFER_SIZE);
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
            BufferedReader reader;
            for (String fileName : this.sourceFiles) {
                LOG.info("reading file : [{}]", fileName);
                reader = FileHelper.readCompressFile(fileName, encoding, bufferSize);
                StorageReaderUtil.doReadFromStream(reader, fileName, readerSliceConfig, recordSender, getTaskPluginCollector());
                recordSender.flush();
                IOUtils.closeQuietly(reader, null);
            }
            LOG.debug("end read source files...");
        }
    }
}
