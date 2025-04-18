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

package com.wgzhao.addax.plugin.reader.dbfreader;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.ColumnEntry;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Created by zhongtian.hu on 19-8-8.
 */
public class DbfReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> path = null;

        private List<String> sourceFiles;

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            StorageReaderUtil.validateParameter(this.originConfig);
            this.validateParameter();
        }

        private void validateParameter()
        {
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            REQUIRED_VALUE,
                            "Nothing to read due to no file path specified.");
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");

            this.sourceFiles = FileHelper.buildSourceTargets(this.path);

            LOG.info("{} file(s) to be read", this.sourceFiles.size());
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
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR, "can not find any files to read in " + this.originConfig.getString(Key.PATH));
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
            LOG.debug("begin reading dbf files...");
            String encode = readerSliceConfig.getString(Key.ENCODING);
            String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
            List<ColumnEntry> column = StorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
            if (column == null || column.isEmpty()) {
                // get column description from dbf file
                column = getColumnInfo(this.sourceFiles.get(0), encode);
            }
            if (column == null) {
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR, "can not retrieve the field information from " + this.sourceFiles.get(0));
            }
            int colNum = column.size();
            DBFRow row;
            for (String fileName : this.sourceFiles) {
                if (StringUtils.isBlank(fileName)) {
                    LOG.warn("source file name is blank, continue...");
                    continue;
                }
                LOG.info("begin reading file : [{}]", fileName);
                try (DBFReader reader = new DBFReader(new FileInputStream(fileName), Charset.forName(encode))) {
                    while ((row = reader.nextRow()) != null) {
                        String[] sourceLine = new String[colNum];
                        for (int i = 0; i < colNum; i++) {
                            // constant value ?
                            if (column.get(i) != null && column.get(i).getValue() != null) {
                                sourceLine[i] = column.get(i).getValue();
                            }
                            else if (row.getString(i) != null && "date".equalsIgnoreCase(column.get(i).getType())) {
                                // DBase's date type does not include time part
                                sourceLine[i] = new SimpleDateFormat("yyyy-MM-dd").format(row.getDate(i));
                            }
                            else {
                                sourceLine[i] = row.getString(i);
                            }
                        }
                        StorageReaderUtil.transportOneRecord(recordSender, column, sourceLine, nullFormat, this.getTaskPluginCollector());
                    }
                }
                catch (FileNotFoundException e) {
                    LOG.error("FileNotFoundException occurred: ", e);
                }
            }
            LOG.debug("end reading dbf files...");
        }

        /**
         * get column description from dbf file
         *
         * @param fPath dbf file path
         * @param encoding the dbf file encoding
         * @return list of column entry
         */
        private List<ColumnEntry> getColumnInfo(String fPath, String encoding)
        {
            List<ColumnEntry> column = new ArrayList<>();
            DBFDataType type;
            try (DBFReader reader = new DBFReader(new FileInputStream(fPath), Charset.forName(encoding))) {
                for (int i = 0; i < reader.getFieldCount(); i++) {
                    ColumnEntry columnEntry = new ColumnEntry();
                    columnEntry.setIndex(i);
                    type = reader.getField(i).getType();
                    switch (type) {
                        case DATE:
                            columnEntry.setType("date");
                            break;
                        case NUMERIC:
                            if (reader.getField(i).getDecimalCount() > 0) {
                                columnEntry.setType("double");
                            }
                            else {
                                columnEntry.setType("long");
                            }
                            break;
                        case FLOATING_POINT:
                        case DOUBLE:
                            columnEntry.setType("double");
                            break;
                        case LOGICAL:
                            columnEntry.setType("boolean");
                            break;
                        case MEMO:
                        case BINARY:
                        case BLOB:
                        case GENERAL_OLE:
                        case PICTURE:
                        case VARBINARY:
                            columnEntry.setType("bytes");
                            break;
                        case CURRENCY:
                            columnEntry.setType("decimal");
                            break;
                        case LONG:
                        case AUTOINCREMENT:
                            columnEntry.setType("long");
                            break;
                        case TIMESTAMP:
                        case TIMESTAMP_DBASE7:
                            columnEntry.setType("timestamp");
                            break;
                        default:
                            columnEntry.setType("string");
                            break;
                    }
                    column.add(columnEntry);
                }
                return column;
            }
            catch (FileNotFoundException e) {
                LOG.error("FileNotFoundException occurred: ", e);
                throw AddaxException.asAddaxException(
                        RUNTIME_ERROR, "can not retrieve the field information from " + fPath);
            }
        }
    }
}
