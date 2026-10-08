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

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.util.RetryUtil;
import com.wgzhao.addax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.wgzhao.addax.storage.util.FileHelper;
import com.wgzhao.addax.storage.writer.StorageWriterUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.wgzhao.addax.core.base.Key.COMPRESS;
import static com.wgzhao.addax.core.base.Key.FILE_FORMAT;
import static com.wgzhao.addax.core.base.Key.FILE_NAME;
import static com.wgzhao.addax.core.base.Key.SUFFIX;
import static com.wgzhao.addax.core.base.Key.WRITE_MODE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Ftp Writer. */
public class FtpWriter
        extends Writer
{
    /**
     * Join a remote path. Remote paths always use the unix separator, whatever platform addax
     * runs on.
     *
     * @param path directory of the file
     * @param fileName name of the file
     * @param suffix extension the compression adds, may be null
     * @return the joined path
     */
    private static String buildRemotePath(String path, String fileName, String suffix)
    {
        StringBuilder remotePath = new StringBuilder(path);
        if (!path.endsWith("/")) {
            remotePath.append('/');
        }
        remotePath.append(fileName);
        if (suffix != null) {
            remotePath.append(suffix);
        }
        return remotePath.toString();
    }

    /** Job. */
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig;
        private Set<String> allFileExists = null;

        private FtpProtocol protocol;
        private FtpConnection connection;
        private IFtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.protocol = this.validateParameter();
            StorageWriterUtil.validateParameter(this.writerSliceConfig);
            // files are emitted through writeToStream (commons-compress), so compress is checked here
            StorageWriterUtil.validateCompression(this.writerSliceConfig);
            this.connection = FtpConnection.from(this.writerSliceConfig, this.protocol);

            try {
                RetryUtil.executeWithRetry((Callable<Void>) () -> {
                    this.ftpHelper = this.protocol.connect(this.connection);
                    return null;
                }, 3, 4000, true);
            }
            catch (Exception e) {
                String message = String.format("Failed to connect %s://%s@%s:%s , errorMessage:%s",
                        this.protocol.name().toLowerCase(Locale.ROOT), this.connection.username(),
                        this.connection.host(), this.connection.port(), e.getMessage());
                LOG.error(message);
                throw AddaxException.asAddaxException(
                        LOGIN_ERROR, message, e);
            }
        }

        /**
         * Validate the job configuration and normalize the keys the task side reads.
         *
         * @return the configured protocol
         */
        private FtpProtocol validateParameter()
        {
            this.writerSliceConfig.getNecessaryValue(FILE_NAME, REQUIRED_VALUE);
            String path = this.writerSliceConfig.getNecessaryValue(FtpKey.PATH, REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("The item path [%s] should be configured as absolute path.", path);
                LOG.error(message);
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, message);
            }

            FtpProtocol protocol = FtpProtocol.of(this.writerSliceConfig.getString(FtpKey.PROTOCOL, "ftp"));
            this.writerSliceConfig.set(FtpKey.PROTOCOL, protocol.name());

            if (protocol == FtpProtocol.FTP) {
                String connectPattern = this.writerSliceConfig.getString(FtpKey.CONNECT_PATTERN,
                        FtpConstant.DEFAULT_FTP_CONNECT_PATTERN);
                if (!"PORT".equalsIgnoreCase(connectPattern) && !"PASV".equalsIgnoreCase(connectPattern)) {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            "Only PORT and PASV connect patterns are supported, the " + connectPattern + " is not.");
                }
                this.writerSliceConfig.set(FtpKey.CONNECT_PATTERN, connectPattern.toUpperCase(Locale.ROOT));
                // a private key is sftp only, drop it so the task side cannot pick it up
                this.writerSliceConfig.set(FtpKey.KEY_PATH, null);
                this.writerSliceConfig.set(FtpKey.KEY_PASS, null);
            }
            else if (this.writerSliceConfig.getBool(FtpKey.USE_KEY, false)) {
                String privateKey = this.writerSliceConfig.getString(FtpKey.KEY_PATH, FtpConstant.DEFAULT_PRIVATE_KEY);
                // expand the home directory by hand, File cannot resolve it
                if (privateKey.startsWith("~")) {
                    privateKey = System.getProperty("user.home") + privateKey.substring(1);
                }
                File keyFile = new File(privateKey);
                if (!keyFile.isFile()) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The private ssh key " + privateKey + " does not exist, check the keyPath or set useKey to false.");
                }
                if (!keyFile.canRead()) {
                    throw AddaxException.asAddaxException(PERMISSION_ERROR,
                            "The private ssh key " + privateKey + " is not readable.");
                }
                this.writerSliceConfig.set(FtpKey.KEY_PATH, privateKey);
            }
            else {
                // useKey is off: the plugin must not try a private key at all, so the keys are
                // dropped from the configuration the tasks read
                this.writerSliceConfig.set(FtpKey.KEY_PATH, null);
                this.writerSliceConfig.set(FtpKey.KEY_PASS, null);
            }
            return protocol;
        }

        @Override
        public void prepare()
        {
            String path = this.writerSliceConfig.getString(FtpKey.PATH);
            // the tasks write into this directory, it has to be there before they start
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
                    fullFileNameToDelete.add(buildRemotePath(path, each, null));
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
                            String.format("The directory [%s] is not empty with writeMode nonConflict, it contains the file(s) with prefix [%s]: [%s]",
                                    path, fileName, StringUtils.join(allFilesInDir.iterator(), ", ")));
                }
            }
            else {
                throw AddaxException
                        .asAddaxException(
                                NOT_SUPPORT_TYPE,
                                String.format("Only truncate, append and nonConflict are supported as writeMode, but [%s] is configured",
                                        writeMode));
            }
        }

        @Override
        public void destroy()
        {
            if (this.ftpHelper == null) {
                return;
            }
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                String message = String.format("Failed to disconnect the server %s:%s, errorMessage:%s",
                        this.connection.host(), this.connection.port(), e.getMessage());
                LOG.error(message, e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return StorageWriterUtil.split(this.writerSliceConfig, this.allFileExists, mandatoryNumber);
        }
    }

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;
        private String fileName;
        /** the extension the compression adds to the file name, e.g. ".gz", empty when not compressed */
        private String suffix = "";

        private FtpConnection connection;
        private IFtpHelper ftpHelper = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(FtpKey.PATH);
            this.fileName = this.buildFileName();
            this.suffix = FileHelper.getCompressFileSuffix(this.writerSliceConfig.getString(COMPRESS));

            FtpProtocol protocol = FtpProtocol.of(this.writerSliceConfig.getString(FtpKey.PROTOCOL, "ftp"));
            this.connection = FtpConnection.from(this.writerSliceConfig, protocol);
            try {
                RetryUtil.executeWithRetry((Callable<Void>) () -> {
                    this.ftpHelper = protocol.connect(this.connection);
                    return null;
                }, 3, 4000, true);
            }
            catch (Exception e) {
                String message = String.format("Failed to connect %s://%s@%s:%s, errorMessage:%s",
                        protocol.name().toLowerCase(Locale.ROOT), this.connection.username(),
                        this.connection.host(), this.connection.port(), e.getMessage());
                LOG.error(message);
                throw AddaxException.asAddaxException(
                        LOGIN_ERROR, message, e);
            }

            // nothing is cleaned before an append run, so a file a previous run left behind with
            // the same name must not be written into; take a fresh name beside it instead
            if ("append".equals(this.writerSliceConfig.getString(WRITE_MODE))) {
                this.fileName = this.avoidNameConflict();
            }
        }

        /**
         * Build the name the task writes. split() hands out names that already carry the
         * extension, the configured name does not.
         *
         * @return the file name including its extension
         */
        private String buildFileName()
        {
            String name = this.writerSliceConfig.getString(FILE_NAME);
            if (name.contains(".")) {
                return name;
            }
            String extension = this.writerSliceConfig.getString(SUFFIX);
            if (extension == null) {
                extension = this.writerSliceConfig.getString(FILE_FORMAT, "txt");
            }
            // a configured extension may or may not carry its dot
            if (extension.startsWith(".")) {
                extension = extension.substring(1);
            }
            return name + "." + extension;
        }

        /**
         * Rename the file when the server already holds one with that name.
         *
         * @return the name to write, the original one when it is free
         */
        private String avoidNameConflict()
        {
            int dot = this.fileName.lastIndexOf('.');
            String prefix = dot < 0 ? this.fileName : this.fileName.substring(0, dot);
            String extension = dot < 0 ? "" : this.fileName.substring(dot);
            String uniqueName = this.fileName;
            while (this.ftpHelper.exists(buildRemotePath(this.path, uniqueName, this.suffix))) {
                uniqueName = String.format("%s_%s%s", prefix, FileHelper.generateFileMiddleName(), extension);
            }
            if (!uniqueName.equals(this.fileName)) {
                LOG.info("The file [{}] already exists, write to [{}] instead.", this.fileName, uniqueName);
            }
            return uniqueName;
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            String fileFullPath = buildRemotePath(this.path, this.fileName, this.suffix);
            LOG.info("begin do write [{}] ...", fileFullPath);

            OutputStream outputStream = null;
            try {
                outputStream = this.ftpHelper.getOutputStream(fileFullPath);
                StorageWriterUtil.writeToStream(lineReceiver, outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }
            finally {
                IOUtils.closeQuietly(outputStream, null);
            }
            // writeToStream closes the stream and with it the transfer; the server's confirmation
            // still has to be read before the connection may be used again
            this.ftpHelper.completePendingCommand();
            LOG.info("end do write");
        }

        @Override
        public void destroy()
        {
            if (this.ftpHelper == null) {
                return;
            }
            try {
                this.ftpHelper.logoutFtpServer();
            }
            catch (Exception e) {
                String message = String.format("Failed to close the ftp connection, host:%s, username:%s, port:%s, errorMessage:%s",
                        this.connection.host(), this.connection.username(), this.connection.port(), e.getMessage());
                LOG.error(message, e);
            }
        }
    }
}
