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
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
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

/**
 * Created by haiwei.luo on 14-9-17.
 */
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
                LOG.warn("您使用format配置日期格式化, 这是不推荐的行为, 请优先使用dateFormat配置项, 两项同时存在则使用dateFormat.");
            }
            StorageWriterUtil.validateParameter(this.writerSliceConfig);
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(FILE_NAME, TxtFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(PATH, TxtFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw AddaxException.asAddaxException(
                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.", path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw AddaxException.asAddaxException(TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                String.format("您指定的文件路径 : [%s] 创建失败.", path));
                    }
                }
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void prepare()
        {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig.getString(FILE_NAME);
            String writeMode = this.writerSliceConfig.getString(WRITE_MODE);

            assert FileHelper.checkDirectoryWritable(path);
            File dir = new File(path);
            // truncate option handler
            if ("truncate".equalsIgnoreCase(writeMode) || "overwrite".equalsIgnoreCase(writeMode)) {
                LOG.info("由于您配置了writeMode truncate/overwrite, 开始清理 [{}] 下面以 [{}] 开头的内容", path, fileName);
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
                    throw AddaxException.asAddaxException(TxtFileWriterErrorCode.WRITE_FILE_ERROR,
                            String.format("您配置的目录清空时出现空指针异常 : [%s]", path), npe);
                }
                catch (IllegalArgumentException iae) {
                    throw AddaxException.asAddaxException(TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 : [%s]", path));
                }
                catch (IOException e) {
                    throw AddaxException.asAddaxException(TxtFileWriterErrorCode.WRITE_FILE_ERROR,
                            String.format("无法清空目录 : [%s]", path), e);
                }
            }
            else if ("append".equals(writeMode)) {
                LOG.info("由于您配置了writeMode append, 写入前不做清理工作, [{}] 目录下写入相应文件名前缀 [{}] 的文件", path, fileName);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("由于您配置了writeMode nonConflict, 开始检查 [{}] 下面的内容", path);
                // warn: check two times about exists, mkdir
                if (dir.exists()) {
                    if (!dir.canRead()) {
                        throw AddaxException.asAddaxException(TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                                String.format("您没有权限查看目录 : [%s]", path));
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
                        LOG.error("冲突文件列表为: [{}]", StringUtils.join(allFiles, ","));
                        throw AddaxException.asAddaxException(TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                String.format("您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
                    }
                }
                else {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw AddaxException.asAddaxException(TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                String.format("您指定的文件路径 : [%s] 创建失败.", path));
                    }
                }
            }
            else {
                throw AddaxException.asAddaxException(TxtFileWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]", writeMode));
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
            Set<String> allFiles;
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles = new HashSet<>(Arrays.asList(Objects.requireNonNull(dir.list())));
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
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
            suffix = FileHelper.getCompressFileSuffix(compress);

            this.fileName = this.fileName + "." + this.fileFormat;
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info("write to file : [{}]", fileFullPath);

            OutputStream outputStream = null;

            try {
                File newFile = new File(fileFullPath);
                assert newFile.createNewFile();
                outputStream = new FileOutputStream(newFile);
                StorageWriterUtil.writeToStream(lineReceiver, outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            }
            catch (IOException ioe) {
                throw AddaxException.asAddaxException(TxtFileWriterErrorCode.WRITE_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), ioe);
            }
            finally {
                IOUtils.closeQuietly(outputStream, null);
            }
            LOG.info("end do write");
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
            return String.format("%s%s%s", this.path, this.fileName, this.suffix);
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
