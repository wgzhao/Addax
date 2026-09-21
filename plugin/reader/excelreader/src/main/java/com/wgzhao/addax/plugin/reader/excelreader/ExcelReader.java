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

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Excel Reader. */
public class ExcelReader
        extends Reader
{
    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;
        private List<String> sourceFiles;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            // the path is either one string or a list of them, the string form came first
            Object pathValue = this.originConfig.get(Key.PATH);
            if (pathValue == null || String.valueOf(pathValue).isBlank()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The required item 'path' is not found");
            }
            List<String> path = pathValue instanceof List<?> list
                    ? list.stream().map(String::valueOf).toList()
                    : List.of(String.valueOf(pathValue));

            this.sourceFiles = FileHelper.buildSourceTargets(path);
            if (sourceFiles.isEmpty()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Cannot find any file in path: " + path + ", assuring the path(s) exists and has right permission");
            }
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

            int splitNumber = Math.min(this.sourceFiles.size(), adviceNumber);

            List<List<String>> splitSourceFiles = FileHelper.splitSourceFiles(this.sourceFiles, splitNumber);
            for (List<String> files : splitSourceFiles) {
                Configuration splitConfig = this.originConfig.clone();
                splitConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitConfig);
            }
            LOG.debug("Split finished...");
            return readerSplitConfigs;
        }
    }

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private List<String> sourceFiles;
        private ExcelHelper.Options options;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
            boolean header = readerSliceConfig.getBool(Key.HEADER, false);
            int skipRows = readerSliceConfig.getInt("skipRows", 0);
            boolean trim = readerSliceConfig.getBool("trim", true);
            String sheetName = readerSliceConfig.getString("sheetName", null);
            int sheetIndex = readerSliceConfig.getInt("sheetIndex", 0);
            if (sheetName != null && sheetName.isBlank()) {
                sheetName = null;
            }
            if (sheetName == null && sheetIndex < 0) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "sheetIndex must not be negative");
            }
            this.options = new ExcelHelper.Options(header, skipRows, trim, sheetName, sheetIndex);
            if (header) {
                LOG.info("The first row is skipped as a table header");
            }
            if (skipRows > 0) {
                LOG.info("The first {} rows are skipped", skipRows);
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
                Path path = Path.of(file);
                try {
                    // a directory beside the workbooks may hold anything, only a file that starts
                    // with a workbook magic is worth opening
                    if (!Files.isRegularFile(path) || !ExcelHelper.isExcel(path)) {
                        LOG.warn("Skip {}, it is not an Excel file", file);
                        continue;
                    }
                    LOG.info("Begin to read file {}", file);
                    try (ExcelHelper helper = ExcelHelper.open(path, options)) {
                        helper.read(recordSender::createRecord, recordSender::sendToWriter);
                    }
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(IO_ERROR,
                            "Failed to read " + file + ": " + e.getMessage(), e);
                }
            }
        }
    }
}
