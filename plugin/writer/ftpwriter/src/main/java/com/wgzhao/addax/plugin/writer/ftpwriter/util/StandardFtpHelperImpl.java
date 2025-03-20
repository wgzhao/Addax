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

package com.wgzhao.addax.plugin.writer.ftpwriter.util;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;

public class StandardFtpHelperImpl
        implements IFtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelperImpl.class);
    FTPClient ftpClient = null;

    @Override
    public void loginFtpServer(String host, int port, String username, String password, String keyPath, String keyPass, int timeout)
    {
        this.ftpClient = new FTPClient();
        try {
            this.ftpClient.setControlEncoding("UTF-8");
            this.ftpClient.setDefaultTimeout(timeout);
            this.ftpClient.setConnectTimeout(timeout);
            this.ftpClient.setDataTimeout(Duration.ofSeconds(timeout));

            this.ftpClient.connect(host, port);
            this.ftpClient.login(username, password);

            this.ftpClient.enterRemotePassiveMode();
            this.ftpClient.enterLocalPassiveMode();
            // Always use binary transfer mode
            this.ftpClient.setFileType(BINARY_FILE_TYPE);
            int reply = this.ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                this.ftpClient.disconnect();
                throw AddaxException.asAddaxException(
                        LOGIN_ERROR, "Failed to connect ftp server" );
            }
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(
                    LOGIN_ERROR, "Failed to connect the ftp server", e);
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (this.ftpClient.isConnected()) {
            try {
                this.ftpClient.logout();
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(
                        CONNECT_ERROR, "Failed to disconnect", e);
            }
            finally {
                if (this.ftpClient.isConnected()) {
                    try {
                        this.ftpClient.disconnect();
                    }
                    catch (IOException e) {
                        LOG.error("Failed to disconnect", e);
                    }
                }
                this.ftpClient = null;
            }
        }
    }

    @Override
    public void mkdir(String directoryPath)
    {
        try {
            this.printWorkingDirectory();
            boolean isDirExist = this.ftpClient.changeWorkingDirectory(directoryPath);
            if (!isDirExist) {
                int replayCode = this.ftpClient.mkd(directoryPath);
                if (replayCode != FTPReply.COMMAND_OK && replayCode != FTPReply.PATHNAME_CREATED) {
                    throw AddaxException.asAddaxException(
                            EXECUTE_FAIL,
                            "Failed to create directory, please check the permission");
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to create directory", e);
        }
    }

    @Override
    public void mkDirRecursive(String directoryPath)
    {
        StringBuilder dirPath = new StringBuilder();
        dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
        String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
        try {
            for (String dirName : dirSplit) {
                dirPath.append(dirName);
                boolean mkdirSuccess = mkDirSingleHierarchy(dirPath.toString());
                dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                if (!mkdirSuccess) {
                    throw AddaxException.asAddaxException(EXECUTE_FAIL, "Failed to create directory");
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to create directory", e);
        }
    }

    public boolean mkDirSingleHierarchy(String directoryPath)
            throws IOException
    {
        boolean isDirExist = this.ftpClient
                .changeWorkingDirectory(directoryPath);
        if (!isDirExist) {
            int replayCode = this.ftpClient.mkd(directoryPath);
            return replayCode == FTPReply.COMMAND_OK || replayCode == FTPReply.PATHNAME_CREATED;
        }
        return true;
    }

    @Override
    public OutputStream getOutputStream(String filePath)
    {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0, StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.ftpClient.changeWorkingDirectory(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.ftpClient.appendFileStream(filePath);
            if (null == writeOutputStream) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, "Failed to open file for writing");
            }

            return writeOutputStream;
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to open file for writing", e);
        }
    }

    @Override
    public String getRemoteFileContent(String filePath)
    {
        try {
            this.completePendingCommand();
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0, StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.ftpClient.changeWorkingDirectory(parentDir);
            this.printWorkingDirectory();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(22);
            this.ftpClient.retrieveFile(filePath, outputStream);
            String result = outputStream.toString();
            IOUtils.closeQuietly(outputStream, null);
            return result;
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to get file content", e);
        }
    }

    @Override
    public Set<String> getAllFilesInDir(String dir, String prefixFileName)
    {
        Set<String> allFilesWithPointedPrefix = new HashSet<>();
        try {
            boolean isDirExist = this.ftpClient.changeWorkingDirectory(dir);
            if (!isDirExist) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, "the directory " + dir + " does not exist");
            }
            this.printWorkingDirectory();
            FTPFile[] fs = this.ftpClient.listFiles(dir);
            LOG.debug("list files in  {}", JSON.toJSONString(fs, JSONWriter.Feature.UseSingleQuotes));
            for (FTPFile ff : fs) {
                String strName = ff.getName();
                if (strName.startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(strName);
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to change working directory", e);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete)
    {
        boolean deleteOk;
        this.printWorkingDirectory();
        try {
            for (String each : filesToDelete) {
                LOG.info("Try to delete file {}", each);
                deleteOk = this.ftpClient.deleteFile(each);
                if (!deleteOk) {
                    throw AddaxException.asAddaxException(
                            IO_ERROR,
                            "Failed to delete file, please check the permission");
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, "Failed to delete file", e);
        }
    }

    private void printWorkingDirectory()
    {
        try {
            LOG.info("current working directory:{}", this.ftpClient.printWorkingDirectory());
        }
        catch (Exception e) {
            LOG.warn("printWorkingDirectory error:{}", e.getMessage());
        }
    }

    @Override
    public void completePendingCommand()
    {
        /*
         * Q:After I perform a file transfer to the server,
         * printWorkingDirectory() returns null. A:You need to call
         * completePendingCommand() after transferring the file. wiki:
         * http://wiki.apache.org/commons/Net/FrequentlyAskedQuestions
         */
        try {
            boolean isOk = this.ftpClient.completePendingCommand();
            if (!isOk) {
                throw AddaxException.asAddaxException(
                        EXECUTE_FAIL,
                        "Failed to complete the pending command, please check the permission");
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(
                    EXECUTE_FAIL, "Failed to complete the pending command", e);
        }
    }
}
