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

package com.wgzhao.addax.plugin.writer.ftpwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.util.RetryUtil;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.SftpHelperImpl;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.StandardFtpHelperImpl;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_ENCODING;
import static com.wgzhao.addax.common.base.Key.COMPRESS;
import static com.wgzhao.addax.common.base.Key.ENCODING;
import static com.wgzhao.addax.common.base.Key.FILE_FORMAT;
import static com.wgzhao.addax.common.base.Key.FILE_NAME;
import static com.wgzhao.addax.common.base.Key.SUFFIX;
import static com.wgzhao.addax.common.base.Key.WRITE_MODE;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;

public class FtpWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private static final int DEFAULT_FTP_PORT = 21;
        private static final int DEFAULT_SFTP_PORT = 22;
        private static final int DEFAULT_TIMEOUT = 60000;
        private static final String DEFAULT_PRIVATE_KEY = "~/.ssh/id_rsa";

        private Configuration writerSliceConfig = null;
        private Set<String> allFileExists = null;

        private String host;
        private int port;
        private String protocol;
        private String username;
        private String password;
        private int timeout;

        private IFtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            StorageWriterUtil.validateParameter(this.writerSliceConfig);
            String keyPath = this.writerSliceConfig.getString(FtpKey.KEY_PATH, null);
            String keyPass = this.writerSliceConfig.getString(FtpKey.KEY_PASS, null);

            try {
                RetryUtil.executeWithRetry((Callable<Void>) () -> {
                    ftpHelper.loginFtpServer(host, port, username, password, keyPath, keyPass, timeout);
                    return null;
                }, 3, 4000, true);
            }
            catch (Exception e) {
                String message = String.format("Failed to connect %s://%s@%s:%s , errorMessage:%s",
                        protocol, username, host, port, e.getMessage());
                LOG.error(message);
                throw AddaxException.asAddaxException(
                        LOGIN_ERROR, message, e);
            }
        }

        private void validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(FILE_NAME, REQUIRED_VALUE);
            String path = this.writerSliceConfig.getNecessaryValue(FtpKey.PATH, REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("The item path [%s] should be configured as absolute path.", path);
                LOG.error(message);
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, message);
            }

            this.host = this.writerSliceConfig.getNecessaryValue(FtpKey.HOST, REQUIRED_VALUE);
            this.username = this.writerSliceConfig.getNecessaryValue(FtpKey.USERNAME, REQUIRED_VALUE);
            this.password = this.writerSliceConfig.getString(FtpKey.PASSWORD, null);
            this.timeout = this.writerSliceConfig.getInt(FtpKey.TIMEOUT, DEFAULT_TIMEOUT);

            this.protocol = this.writerSliceConfig.getString(FtpKey.PROTOCOL, "ftp");
            if (!("ftp".equalsIgnoreCase(protocol) || "sftp".equalsIgnoreCase(protocol))) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        protocol + " is unsupported, supported protocol are ftp and sftp");
            }
            this.writerSliceConfig.set(FtpKey.PROTOCOL, protocol);
            if ("sftp".equalsIgnoreCase(protocol)) {
                this.port = this.writerSliceConfig.getInt(FtpKey.PORT, DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelperImpl();
                // use ssh private key or not ?
                boolean useKey = this.writerSliceConfig.getBool(FtpKey.USE_KEY, false);
                if (useKey) {
                    String privateKey = this.writerSliceConfig.getString(FtpKey.KEY_PATH, DEFAULT_PRIVATE_KEY);
                    // check privateKey does exist or not
                    if (privateKey.startsWith("~")) {
                        // expand home directory
                        privateKey = privateKey.replaceFirst("^~", System.getProperty("user.home"));
                        // does it exist?
                        boolean isFile = new File(privateKey).isFile();
                        if (isFile) {
                            this.writerSliceConfig.set(FtpKey.KEY_PATH, privateKey);
                        }
                        else {
                            String msg = "You have configured to use the key, but neither the configured key file nor the default file(" +
                                    DEFAULT_PRIVATE_KEY + " exists";
                            throw AddaxException.asAddaxException(ILLEGAL_VALUE, msg);
                        }
                    }
                }
            }
            else if ("ftp".equalsIgnoreCase(protocol)) {
                this.port = this.writerSliceConfig.getInt(FtpKey.PORT, DEFAULT_FTP_PORT);
                // login with private key is unavailable for ftp protocol, disable it.
                this.writerSliceConfig.set(FtpKey.KEY_PATH, null);
                this.ftpHelper = new StandardFtpHelperImpl();
            }
            else {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE, protocol + " is unsupported, supported protocol are ftp and sftp");
            }
            this.writerSliceConfig.set(FtpKey.PORT, this.port);
        }

        @Override
        public void prepare()
        {
            String path = this.writerSliceConfig.getString(FtpKey.PATH);
            // warn: 这里用户需要配一个目录
            this.ftpHelper.mkDirRecursive(path);

            String fileName = this.writerSliceConfig.getString(FILE_NAME);
            String writeMode = this.writerSliceConfig.getString(WRITE_MODE);

            Set<String> allFilesInDir = this.ftpHelper.getAllFilesInDir(path, fileName);
            this.allFileExists = allFilesInDir;

            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info("The current writeMode is truncate, begin to cleanup all files with prefix [{}] under [{}].", fileName, path);
                Set<String> fullFileNameToDelete = new HashSet<>();
                for (String each : allFilesInDir) {
                    fullFileNameToDelete.add(StorageWriterUtil.buildFilePath(path, each, null));
                }
                LOG.info("The following file(s) will be deleted: [{}].", StringUtils.join(fullFileNameToDelete.iterator(), ", "));

                this.ftpHelper.deleteFiles(fullFileNameToDelete);
            }
            else if ("append".equals(writeMode)) {
                LOG.info("The current writeMode is append, no cleanup is performed. It will write file(s) with prefix [{}] under [{}].",
                        fileName, path);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("The current writeMode is noConflict, begin to check directory [{}] is empty or not", path);
                if (!allFilesInDir.isEmpty()) {
                    LOG.info("The directory [{}] includes the following files with prefix [{}]: [{}].", path, fileName,
                            StringUtils.join(allFilesInDir.iterator(), ", "));
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
                }
            }
            else {
                throw AddaxException
                        .asAddaxException(
                                ILLEGAL_VALUE,
                                String.format("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
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
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                String message = String.format("Failed to disconnect server %s:%s, errorMessage:%s", host, port, e.getMessage());
                LOG.error(message, e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return StorageWriterUtil.split(this.writerSliceConfig, this.allFileExists, mandatoryNumber);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final int DEFAULT_TIMEOUT = 60000;

        private Configuration writerSliceConfig;

        private String path;
        private String fileName;
        private String suffix;

        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;
        private String compress;
        private IFtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(FtpKey.PATH);
            StringBuilder realFileName = new StringBuilder();
            realFileName.append(this.writerSliceConfig.getString(FILE_NAME));
            String fileFormat = this.writerSliceConfig.getString(FILE_FORMAT, "txt");
            this.suffix = this.writerSliceConfig.getString(SUFFIX);
            if (this.suffix != null) {
                realFileName.append(".").append(suffix);
            }
            else {
                realFileName.append(".").append(fileFormat);
            }
            this.compress = this.writerSliceConfig.getString(COMPRESS, null);
            if (this.compress != null) {
                if ("zip".equalsIgnoreCase(this.compress)) {
                    this.suffix = ".zip";
                }
                else if ("gzip".equalsIgnoreCase(this.compress)) {
                    this.suffix = ".gz";
                }
                else if ("bzip2".equalsIgnoreCase(this.compress) || "bzip".equalsIgnoreCase(this.compress)) {
                    this.suffix = ".bz2";
                }
            }
            this.fileName = realFileName.toString();
            this.host = this.writerSliceConfig.getString(FtpKey.HOST);
            this.port = this.writerSliceConfig.getInt(FtpKey.PORT);
            this.username = this.writerSliceConfig.getString(FtpKey.USERNAME);
            this.password = this.writerSliceConfig.getString(FtpKey.PASSWORD);
            this.timeout = this.writerSliceConfig.getInt(FtpKey.TIMEOUT, DEFAULT_TIMEOUT);

            String keyPath = this.writerSliceConfig.getString(FtpKey.KEY_PATH, null);
            String keyPass = this.writerSliceConfig.getString(FtpKey.KEY_PASS, null);
            String protocol = this.writerSliceConfig.getString(FtpKey.PROTOCOL);

            if ("sftp".equalsIgnoreCase(protocol)) {
                this.ftpHelper = new SftpHelperImpl();
            }
            else if ("ftp".equalsIgnoreCase(protocol)) {
                this.ftpHelper = new StandardFtpHelperImpl();
            }
            try {
                RetryUtil.executeWithRetry((Callable<Void>) () -> {
                    ftpHelper.loginFtpServer(host, port, username, password, keyPath, keyPass, timeout);
                    return null;
                }, 3, 4000, true);
            }
            catch (Exception e) {
                String message = String.format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                        host, username, port, e.getMessage());
                LOG.error(message);
                throw AddaxException.asAddaxException(
                        LOGIN_ERROR, message, e);
            }
        }

        @Override
        public void prepare()
        {
            String encoding = writerSliceConfig.getString(ENCODING, DEFAULT_ENCODING);
            // handle blank encoding
            if (StringUtils.isBlank(encoding)) {
                LOG.warn("您配置的encoding为[{}], 使用默认值[{}]", encoding, DEFAULT_ENCODING);
                writerSliceConfig.set(ENCODING, DEFAULT_ENCODING);
            }
            this.compress = writerSliceConfig.getString(COMPRESS);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("begin do write...");
            String fileFullPath = StorageWriterUtil.buildFilePath(path, fileName, suffix);
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            OutputStream outputStream = null;
            try {
                outputStream = ftpHelper.getOutputStream(fileFullPath);
                StorageWriterUtil.writeToStream(lineReceiver, outputStream, writerSliceConfig, fileName, getTaskPluginCollector());
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), e);
            }
            finally {
                IOUtils.closeQuietly(outputStream, null);
            }
            LOG.info("end do write");
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                String message = String.format("关闭与ftp服务器连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                        host, username, port, e.getMessage());
                LOG.error(message, e);
            }
        }
    }
}
