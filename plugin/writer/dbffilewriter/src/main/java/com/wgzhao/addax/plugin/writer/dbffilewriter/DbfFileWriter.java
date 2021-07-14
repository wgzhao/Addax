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

package com.wgzhao.addax.plugin.writer.dbffilewriter;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import java.util.UUID;



/**
 * Created by haiwei.luo on 14-9-17.
 */
public class DbfFileWriter
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
                LOG.warn("您使用format配置日期格式化, 这是不推荐的行为, 请优先使用dateFormat配置项, 两项同时存在则使用dateFormat.");
            }
            validateParameter();
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, DbfFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH, DbfFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw AddaxException.asAddaxException(
                                    DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.", path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw AddaxException.asAddaxException(
                                        DbfFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.", path));
                    }
                }
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(
                        DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
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
                LOG.info("由于您配置了writeMode truncate, 开始清理 [{}] 下面以 [{}] 开头的内容", path, fileName);
                File dir = new File(path);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    if (dir.exists()) {
                        // warn:不要使用FileUtils.deleteQuietly(dir)
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        assert filesWithFileNamePrefix != null;
                        for (File eachFile : filesWithFileNamePrefix) {
                            LOG.info("delete file [{}].", eachFile.getName());
                            FileUtils.forceDelete(eachFile);
                        }
                        // FileUtils.cleanDirectory(dir)
                    }
                }
                catch (NullPointerException npe) {
                    throw AddaxException
                            .asAddaxException(
                                    DbfFileWriterErrorCode.WRITE_FILE_ERROR,
                                    String.format("您配置的目录清空时出现空指针异常 : [%s]", path), npe);
                }
                catch (IllegalArgumentException iae) {
                    throw AddaxException.asAddaxException(
                            DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 : [%s]", path));
                }
                catch (SecurityException se) {
                    throw AddaxException.asAddaxException(
                            DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(
                            DbfFileWriterErrorCode.WRITE_FILE_ERROR,
                            String.format("无法清空目录 : [%s]", path), e);
                }
            }
            else if ("append".equals(writeMode)) {
                LOG.info("由于您配置了writeMode append, 写入前不做清理工作, [{}] 目录下写入相应文件名前缀 [{}] 的文件", path, fileName);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("由于您配置了writeMode nonConflict, 开始检查 [{}] 下面的内容", path);
                // warn: check two times about exists, mkdirs
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw AddaxException
                                    .asAddaxException(
                                            DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.", path));
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
                            LOG.error("冲突文件列表为: [{}]",
                                    StringUtils.join(allFiles, ","));
                            throw AddaxException
                                    .asAddaxException(
                                            DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format("您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
                        }
                    }
                    else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw AddaxException
                                    .asAddaxException(
                                            DbfFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                            String.format("您指定的文件路径 : [%s] 创建失败.", path));
                        }
                    }
                }
                catch (SecurityException se) {
                    throw AddaxException.asAddaxException(
                            DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                }
            }
            else {
                throw AddaxException
                        .asAddaxException(
                                DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                String.format("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]", writeMode));
            }

            // check column configuration is valid or not
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            for (Configuration column: columns) {
                if ( "numeric".equalsIgnoreCase(column.getString(Key.TYPE)) &&
                        (column.getString(Key.PRECISION, null) == null || column.getString(Key.SCALE, null) == null))
                {
                    throw AddaxException.asAddaxException(
                            DbfFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("numeric 类型必须配置 %s 和 %s 项", Key.LENGTH, Key.SCALE)
                    );
                }

                if ("char".equalsIgnoreCase(column.getString(Key.TYPE)) && column.getString(Key.LENGTH, null) == null)
                {
                    throw AddaxException.asAddaxException(
                            DbfFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("char 类型必须配置 %s 项", Key.LENGTH)
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
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String filePrefix = this.writerSliceConfig.getString(Key.FILE_NAME);

            Set<String> allFiles;
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles = new HashSet<>(Arrays.asList(Objects.requireNonNull(dir.list())));
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(
                        DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
            }

            String fileSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig.clone();

                String fullFileName;
                if (mandatoryNumber > 1) {
                    fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                    fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
                    while (allFiles.contains(fullFileName)) {
                        fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                        fullFileName = String.format("%s__%s", filePrefix,
                                fileSuffix);
                    }
                }
                else {
                    fullFileName = filePrefix;
                }
                allFiles.add(fullFileName);

                splitedTaskConfig.set(Key.FILE_NAME, fullFileName);

                LOG.info("split write file name:[{}]", fullFileName);

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
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
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("begin to write...");
            String fileFullPath = this.buildFilePath();
            LOG.info("write to file : [{}]", fileFullPath);
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
                    // Date类型不能设置字段长度，这里没有处理其它没有字段长度的类型
                }
                writer.setFields(fields);

                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    Object[] rowData = new Object[columns.size()];
                    Column column;
                    for (int i = 0; i < columns.size(); i++) {
                        column = record.getColumn(i);
                        if (column != null && null != column.getRawData()) {
                            String colData = column.getRawData().toString();
                            switch (columns.get(i).getString(Key.TYPE)) {
                                case "numeric":
                                    rowData[i] = Float.valueOf(colData);
                                    break;
                                case "char":
                                    //rowData[i] = new String(colData.getBytes("GBK"))
                                    rowData[i] = colData;
                                    break;
                                case "date":
                                    rowData[i] =new Date(Long.parseLong(colData));
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
                        DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            }
            LOG.info("end write");
        }

        private String buildFilePath()
        {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = this.path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = this.path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return String.format("%s%s", this.path, this.fileName);
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
