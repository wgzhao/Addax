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

package com.wgzhao.addax.plugin.writer.txtfilewriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.util.FileHelper;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.wgzhao.addax.common.base.Key.COMPRESS;
import static com.wgzhao.addax.common.base.Key.DATE_FORMAT;
import static com.wgzhao.addax.common.base.Key.FILE_FORMAT;
import static com.wgzhao.addax.common.base.Key.FILE_NAME;
import static com.wgzhao.addax.common.base.Key.FORMAT;
import static com.wgzhao.addax.common.base.Key.PATH;
import static com.wgzhao.addax.common.base.Key.WRITE_MODE;
import static com.wgzhao.addax.common.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.spi.ErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class TxtFileWriter
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
            String dateFormatOld = this.writerSliceConfig.getString(FORMAT);
            String dateFormatNew = this.writerSliceConfig.getString(DATE_FORMAT);
            if (null == dateFormatNew) {
                this.writerSliceConfig.set(DATE_FORMAT, dateFormatOld);
            }
            if (null != dateFormatOld) {
                LOG.warn("The `format` item has been deprecated; please use `dateFormat` for configuration.");
            }
            StorageWriterUtil.validateParameter(this.writerSliceConfig);
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(FILE_NAME, REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(PATH, REQUIRED_VALUE);
            File dir;
            dir = new File(path);

            if (dir.isFile()) {
                throw AddaxException.asAddaxException(
                        CONFIG_ERROR,
                        "You need to set the path to a directory, but you set it to a file: " + path);
            }

            if (!dir.exists() && !dir.mkdirs()) {
                throw AddaxException.asAddaxException(
                        EXECUTE_FAIL,
                        "Failed to create directory: " + path);
            }
            else if (!dir.canWrite()) {
                throw AddaxException.asAddaxException(
                        PERMISSION_ERROR,
                        "No write permission on directory: " + path);
            }
        }

        @Override
        public void prepare()
        {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig.getString(FILE_NAME);
            String writeMode = this.writerSliceConfig.getString(WRITE_MODE);

            File dir = new File(path);
            // truncate option handler
            if ("truncate".equalsIgnoreCase(writeMode) || "overwrite".equalsIgnoreCase(writeMode)) {
                LOG.info("You specify [{}] as writeMode, begin to clean history files starts with [{}] under this path [{}].", writeMode, fileName, path);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    FilenameFilter filter = new PrefixFileFilter(fileName);
                    File[] filesWithFileNamePrefix = dir.listFiles(filter);
                    assert filesWithFileNamePrefix != null;
                    for (File eachFile : filesWithFileNamePrefix) {
                        LOG.info("delete file [{}].", eachFile.getName());
                        FileUtils.forceDelete(eachFile);
                    }
                }
                catch (NullPointerException npe) {
                    throw AddaxException.asAddaxException(RUNTIME_ERROR,
                            String.format("NullPointException occurred when clean history files under this path [%s].", path), npe);
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(IO_ERROR,
                            String.format("IOException occurred when clean history files under this path [%s].", path), e);
                }
            }
            else if ("append".equals(writeMode)) {
                LOG.info("You specify [{}] as writeMode, so we will NOT clean history files starts with [{}] under this path [{}].", writeMode, fileName, path);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("You specify [{}] as writeMode, begin to check the files in [{}].", writeMode, path);
                // warn: check two times about exists, mkdir
                if (dir.exists()) {
                    if (!dir.canRead()) {
                        throw AddaxException.asAddaxException(PERMISSION_ERROR,
                                "Permission denied to access path " + path);
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
                        LOG.error("The file(s) [{}] already exists under path [{}] with nonConflict writeMode.", StringUtils.join(allFiles, ","), path);
                        throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                                String.format("The directory [%s] contains files.", path));
                    }
                }
            }
            else {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                        "Only 'truncate', 'append', and 'nonConflict' are supported as write modes, but you provided " + writeMode);
            }
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            Set<String> allFiles;
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles = new HashSet<>(Arrays.asList(Objects.requireNonNull(dir.list())));
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(PERMISSION_ERROR,
                        String.format("Permission denied to access path [%s].", path), se);
            }
            return StorageWriterUtil.split(writerSliceConfig, allFiles, mandatoryNumber);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private String path;
        private String fileName;
        private String fileFormat;
        // add correspond compress suffix if compress is present
        private String suffix = "";

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(PATH);
            this.fileName = this.writerSliceConfig.getString(FILE_NAME);
            this.fileFormat = this.writerSliceConfig.getString(FILE_FORMAT, "txt");
        }

        @Override
        public void prepare()
        {
            String compress = this.writerSliceConfig.getString(COMPRESS);
            if (!fileName.contains(".")) {
                this.fileName = this.fileName + "." + this.fileFormat;
            }
            suffix = FileHelper.getCompressFileSuffix(compress);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("begin do write...");
            String fileFullPath = StorageWriterUtil.buildFilePath(this.path, this.fileName, this.suffix);
            LOG.info("write to file : [{}]", fileFullPath);

            OutputStream outputStream = null;

            try {
                File newFile = new File(fileFullPath);
                if (!newFile.createNewFile()) {
                    throw new IOException("Failed to create new file: " + fileFullPath);
                }
                outputStream = Files.newOutputStream(newFile.toPath());
                StorageWriterUtil.writeToStream(lineReceiver, outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(PERMISSION_ERROR,
                        String.format("Permission denied to create file [%s].", fileFullPath), se);
            }
            catch (IOException ioe) {
                throw AddaxException.asAddaxException(IO_ERROR,
                        String.format("Fail to create file [%s].", this.fileName), ioe);
            }
            finally {
                IOUtils.closeQuietly(outputStream, null);
            }
            LOG.info("end do write");
        }

        @Override
        public void destroy()
        {
            //
        }
    }
}
