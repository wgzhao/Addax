/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.plugin.reader.excelreader;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ExcelReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;
        private List<String> path = null;
        private List<String> sourceFiles;
        private final Map<String, Pattern> pattern = new HashMap<>();
        private final Map<String, Boolean> isRegexPath = new HashMap<>();

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, ExcelReaderErrorCode.REQUIRED_VALUE);
            if (StringUtils.isBlank(pathInString)) {
                throw AddaxException.asAddaxException(ExcelReaderErrorCode.REQUIRED_VALUE, "the path is required");
            }
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(ExcelReaderErrorCode.REQUIRED_VALUE, "the path is required");
                }
            }

            // warn:make sure this regex string
            // warn:no need trim
            for (String eachPath : this.path) {
                String regexString = eachPath.replace("*", ".*").replace("?", ".?");
                Pattern pattern = Pattern.compile(regexString);
                this.pattern.put(eachPath, pattern);
            }
            this.sourceFiles = this.buildSourceTargets();
            LOG.info("The number of files to read is: [{}]", this.sourceFiles.size());
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("Begin to split...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(
                        ExcelReaderErrorCode.EMPTY_DIR_EXCEPTION,
                        "Nothing found in the directory " + this.originConfig.getString(Key.PATH) + ". Please check it");
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    this.sourceFiles, splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("Split finished...");
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
                this.buildSourceTargetsEachPath(eachPath, parentDirectory, toBeReadFiles);
            }
            return Arrays.asList(toBeReadFiles.toArray(new String[0]));
        }

        private void buildSourceTargetsEachPath(String regexPath, String parentDirectory, Set<String> toBeReadFiles)
        {
            // 检测目录是否存在，错误情况更明确
            try {
                File dir = new File(parentDirectory);
                boolean isExists = dir.exists();
                if (!isExists) {
                    throw AddaxException.asAddaxException(ExcelReaderErrorCode.FILE_NOT_EXISTS,
                            parentDirectory + ": No such file or directory");
                }
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(ExcelReaderErrorCode.SECURITY_NOT_ENOUGH,
                        "Permission denied for directory: " + parentDirectory);
            }

            directoryRover(regexPath, parentDirectory, toBeReadFiles);
        }

        private void directoryRover(String regexPath, String parentDirectory, Set<String> toBeReadFiles)
        {
            File directory = new File(parentDirectory);
            // is a normal file
            if (!directory.isDirectory()) {
                if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
                    if (parentDirectory.endsWith(".xlsx") || parentDirectory.endsWith(".xls")) {
                        toBeReadFiles.add(parentDirectory);
                    } else {
                        LOG.warn("File {} is not valid Excel file, ignore it", parentDirectory);
                    }
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
                        throw AddaxException.asAddaxException(ExcelReaderErrorCode.SECURITY_NOT_ENOUGH, message);
                    }
                }
                catch (SecurityException e) {
                    String message = String.format("您没有权限查看目录 : [%s]", directory);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(ExcelReaderErrorCode.SECURITY_NOT_ENOUGH, message, e);
                }
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

        private boolean isTargetFile(String regexPath, String absoluteFilePath)
        {
            if (this.isRegexPath.get(regexPath)) {
                return this.pattern.get(regexPath).matcher(absoluteFilePath).matches();
            }
            else {
                return true;
            }
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private List<String> sourceFiles;
        private boolean header = false;
        private int skipRows = 0;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
            this.header = readerSliceConfig.getBool("header", false);
            if (this.header) {
                LOG.info("The first row is skipped as a table header");
            }
            this.skipRows = readerSliceConfig.getInt("skipRows", 0);
            if (this.skipRows > 0) {
                LOG.info("The first {} rows is skipped", this.skipRows);
            }
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            for (String file : sourceFiles) {
                LOG.info("begin read file {}", file);
                ExcelHelper excelHelper = new ExcelHelper(header, skipRows);
                excelHelper.open(file);
                Record record = excelHelper.readLine(recordSender.createRecord());
                while (record != null) {
                    recordSender.sendToWriter(record);
                    record = excelHelper.readLine(recordSender.createRecord());
                }
                excelHelper.close();
            }
        }
    }
}
