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

package com.wgzhao.addax.plugin.writer.dbfwriter;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.util.FileHelper;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class DbfWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            String dateFormatOld = this.writerSliceConfig.getString(Key.FORMAT);
            String dateFormatNew = this.writerSliceConfig.getString(Key.DATE_FORMAT);
            if (null == dateFormatNew) {
                this.writerSliceConfig.set(Key.DATE_FORMAT, dateFormatOld);
            }
            if (null != dateFormatOld) {
                LOG.warn("The item 'format' is deprecated. you can use 'dateFormat' to format date type.");
            }
            validateParameter();
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);

            try {
                File dir = new File(path);
                if (dir.isFile()) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            String.format("The path [%s] you configured exists ,but it is file not directory.", path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw AddaxException.asAddaxException(
                                CONFIG_ERROR,
                                String.format("Failed to create directory [%s].", path));
                    }
                }
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(
                        PERMISSION_ERROR,
                        String.format("Permission denied while creating directory [%s].", path), se);
            }
        }

        @Override
        public void prepare()
        {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
            String writeMode = this.writerSliceConfig.getString(Key.WRITE_MODE);
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info("The current writeMode is 'truncate', begin to cleanup all files or sub-directories " +
                        "with prefix is [{}] under [{}].", path, fileName);
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        assert filesWithFileNamePrefix != null;
                        for (File eachFile : filesWithFileNamePrefix) {
                            LOG.info("Delete file [{}].", eachFile.getName());
                            FileUtils.forceDelete(eachFile);
                        }
                    }
                }
                catch (NullPointerException npe) {
                    throw AddaxException
                            .asAddaxException(
                                    RUNTIME_ERROR,
                                    String.format("NPE occurred whiling cleanup [%s].", path), npe);
                }
                catch (IllegalArgumentException iae) {
                    throw AddaxException.asAddaxException(
                            PERMISSION_ERROR,
                            String.format("IllegalArgumentException occurred cleanup [%s].", path));
                }
                catch (SecurityException se) {
                    throw AddaxException.asAddaxException(
                            PERMISSION_ERROR,
                            String.format("Permission denied for cleaning up [%s]", path));
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(
                            IO_ERROR,
                            String.format("IO exception occurred while cleanup [%s]", path), e);
                }
            }
            else if ("append".equals(writeMode)) {
                LOG.info("The current writeMode is append, no cleanup is performed before writing." +
                        "It will write files with prefix [{}] under the directory [{}].", path, fileName);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("The current writeMode is nonConflict, begin to check the directory [{}] is empty or not.", path);
                // warn: check two times about exists, mkdir
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw AddaxException.asAddaxException(
                                    ILLEGAL_VALUE,
                                    String.format("The path [%s] exists, but it is file not directory.", path));
                        }
                        // fileName is not null
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        assert filesWithFileNamePrefix != null;
                        if (filesWithFileNamePrefix.length > 0) {
                            List<String> allFiles = new ArrayList<>();
                            for (File eachFile : filesWithFileNamePrefix) {
                                allFiles.add(eachFile.getName());
                            }
                            LOG.error("The following files are conflict: [{}]", StringUtils.join(allFiles, ","));
                            throw AddaxException.asAddaxException(
                                    ILLEGAL_VALUE,
                                    String.format("The path [%s] is not empty.", path));
                        }
                    }
                    else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw AddaxException.asAddaxException(
                                    EXECUTE_FAIL,
                                    String.format("Failed to create directory [%s].", path));
                        }
                    }
                }
                catch (SecurityException se) {
                    throw AddaxException.asAddaxException(
                            PERMISSION_ERROR,
                            String.format("Permission denied for creating directory [%s]", path));
                }
            }
            else {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        String.format("The item writeMode only supports truncate, append and nonConflict  the [%s] is not supported.", writeMode));
            }

            // check column configuration is valid or not
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            for (Configuration column : columns) {
                if ("numeric".equalsIgnoreCase(column.getString(Key.TYPE)) &&
                        (column.getString(Key.LENGTH, null) == null || column.getString(Key.SCALE, null) == null)) {
                    throw AddaxException.asAddaxException(
                            CONFIG_ERROR,
                            String.format("The numeric type both require configured item %s and %s.", Key.LENGTH, Key.SCALE)
                    );
                }

                if ("char".equalsIgnoreCase(column.getString(Key.TYPE)) && column.getString(Key.LENGTH, null) == null) {
                    throw AddaxException.asAddaxException(
                            CONFIG_ERROR,
                            String.format("The char type require configured item %s.", Key.LENGTH)
                    );
                }
            }
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
        public List<Configuration> split(int mandatoryNumber)
        {
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            LOG.info("Begin doing split...");
            if (mandatoryNumber == 1) {
                writerSplitConfigs.add(this.writerSliceConfig);
                return writerSplitConfigs;
            }

            String filePrefix = this.writerSliceConfig.getString(Key.FILE_NAME).split("\\.")[0];

            Set<String> allFiles;
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles = new HashSet<>(Arrays.asList(Objects.requireNonNull(dir.list())));
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(
                        PERMISSION_ERROR,
                        String.format("Permission denied for viewing directory [%s].", path));
            }

            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitTaskConfig = this.writerSliceConfig.clone();

                String fullFileName;

                fullFileName = String.format("%s__%s.dbf", filePrefix, FileHelper.generateFileMiddleName());
                while (allFiles.contains(fullFileName)) {
                    fullFileName = String.format("%s__%s.dbf", filePrefix, FileHelper.generateFileMiddleName());
                }

                allFiles.add(fullFileName);

                splitTaskConfig.set(Key.FILE_NAME, fullFileName);

                LOG.info("The split wrote files: [{}]", fullFileName);

                writerSplitConfigs.add(splitTaskConfig);
            }
            LOG.info("Finish split.");
            return writerSplitConfigs;
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String fileName;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            // remove file suffix
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME).split("\\.")[0];
            writerSliceConfig.set(Key.FILE_NAME, this.fileName);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("Begin to write...");
            String fileSuffix = ".dbf";
            String fileFullPath = StorageWriterUtil.buildFilePath(path, fileName, fileSuffix);
            LOG.info("Writing file: [{}]", fileFullPath);
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            DBFWriter writer;
            try {
                File f = new File(fileFullPath);
                String charset = this.writerSliceConfig.getString(Key.ENCODING, "GBK");
                writer = new DBFWriter(f, Charset.forName(charset));

                DBFField[] fields = new DBFField[columns.size()];

                for (int i = 0; i < columns.size(); i++) {
                    fields[i] = new DBFField();
                    fields[i].setName(columns.get(i).getString(Key.NAME));
                    switch (columns.get(i).getString(Key.TYPE).toLowerCase()) {
                        case "char":
                            fields[i].setType(DBFDataType.CHARACTER);
                            fields[i].setLength(columns.get(i).getInt(Key.LENGTH));
                            break;
                        case "numeric":
                            fields[i].setType(DBFDataType.NUMERIC);
                            fields[i].setLength(columns.get(i).getInt(Key.LENGTH));
                            fields[i].setDecimalCount(columns.get(i).getInt(Key.SCALE));
                            break;
                        case "date":
                            fields[i].setType(DBFDataType.DATE);
                            break;
                        case "logical":
                            fields[i].setType(DBFDataType.LOGICAL);
                            break;
                        default:
                            LOG.warn("Unknown data type, assume char type");
                            fields[i].setType(DBFDataType.CHARACTER);
                            fields[i].setLength(columns.get(i).getInt(Key.LENGTH));
                            break;
                    }
                }
                writer.setFields(fields);

                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    Object[] rowData = new Object[columns.size()];
                    Column column;
                    for (int i = 0; i < columns.size(); i++) {
                        column = record.getColumn(i);
                        if (column != null && null != column.getRawData() && "null" != column.getRawData()) {
                            String colData = column.getRawData().toString();
                            switch (columns.get(i).getString(Key.TYPE)) {
                                case "numeric":
                                    rowData[i] = Float.valueOf(colData);
                                    break;
                                case "char":
                                    rowData[i] = colData;
                                    break;
                                case "date":
                                    rowData[i] = new Date(Long.parseLong(colData));
                                    break;
                                case "logical":
                                    rowData[i] = Boolean.parseBoolean(colData);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    writer.addRecord(rowData);
                }

                writer.close();
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(
                        PERMISSION_ERROR,
                        String.format("Permission denied for create directory [%s].", this.fileName));
            }
            LOG.info("Writing finished");
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
