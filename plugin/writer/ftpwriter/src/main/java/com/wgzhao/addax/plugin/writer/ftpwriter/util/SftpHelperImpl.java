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
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;

public class SftpHelperImpl
        implements IFtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelperImpl.class);

    private Session session = null;
    private ChannelSftp channelSftp = null;

    @Override
    public void loginFtpServer(String host, int port, String username, String password, String keyPath, String keyPass, int timeout)
    {
        JSch jsch = new JSch();
        if (keyPath != null) {
            try {
                if (keyPass != null) {
                    jsch.addIdentity(keyPath, keyPass);
                }
                else {
                    jsch.addIdentity(keyPath);
                }
            }
            catch (JSchException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Failed to use private key", e);
            }
        }
        try {
            this.session = jsch.getSession(username, host, port);
            if (this.session == null) {
                throw AddaxException.asAddaxException(LOGIN_ERROR,
                        String.format("Failed to connect %s:%s via sftp protocol", host, port));
            }

            this.session.setPassword(password);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            // config.put("PreferredAuthentications", "password");
            this.session.setConfig(config);
            this.session.setTimeout(timeout);
            this.session.connect();

            this.channelSftp = (ChannelSftp) this.session.openChannel("sftp");
            this.channelSftp.connect();
        }
        catch (JSchException e) {
            String message = String.format("Failed to connect %s:%s because: %s", host, port, e.getMessage());
            LOG.error(message);
            throw AddaxException.asAddaxException(LOGIN_ERROR, message, e);
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (this.channelSftp != null) {
            this.channelSftp.disconnect();
            this.channelSftp = null;
        }
        if (this.session != null) {
            this.session.disconnect();
            this.session = null;
        }
    }

    @Override
    public void mkdir(String directoryPath)
    {
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        }
        catch (SftpException e) {
            if (e.getMessage().equalsIgnoreCase("no such file")) {
                LOG.warn("The directory {} does not exists, try to create it", directoryPath);
            }
        }
        if (!isDirExist) {
            try {
                // warn 检查mkdir -p
                this.channelSftp.mkdir(directoryPath);
            }
            catch (SftpException e) {
                LOG.error("IOException occurred while create folder {}, {}", directoryPath, e);
                throw AddaxException.asAddaxException(IO_ERROR, e);
            }
        }
    }

    @Override
    public void mkDirRecursive(String directoryPath)
    {
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        }
        catch (SftpException e) {
            if (e.getMessage().equalsIgnoreCase("no such file")) {
                LOG.warn("The directory {} does not exists, try to create it", directoryPath);
            }
        }
        if (!isDirExist) {
            StringBuilder dirPath = new StringBuilder();
            dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
            String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
            try {
                for (String dirName : dirSplit) {
                    dirPath.append(dirName);
                    mkDirSingleHierarchy(dirPath.toString());
                    dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                }
            }
            catch (SftpException e) {
                LOG.error("IOException occurred while create folder {}, {}", directoryPath, e);
                throw AddaxException.asAddaxException(IO_ERROR, e);
            }
        }
    }

    public void mkDirSingleHierarchy(String directoryPath)
            throws SftpException
    {
        boolean isDirExist = false;
        try {
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        }
        catch (SftpException e) {
            LOG.info("creating folder {}", directoryPath);
            this.channelSftp.mkdir(directoryPath);
        }
        if (!isDirExist) {
            LOG.info("creating folder {}", directoryPath);
            this.channelSftp.mkdir(directoryPath);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath)
    {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0, StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.channelSftp.put(filePath, ChannelSftp.APPEND);
            if (null == writeOutputStream) {
                throw AddaxException.asAddaxException(IO_ERROR, "failed to write file " + filePath);
            }
            return writeOutputStream;
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "failed to write file " + filePath);
        }
    }

    @Override
    public String getRemoteFileContent(String filePath)
    {
        try {
            this.completePendingCommand();
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0, StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(22);
            this.channelSftp.get(filePath, outputStream);
            String result = outputStream.toString();
            IOUtils.closeQuietly(outputStream, null);
            return result;
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "failed to write file " + filePath);
        }
    }

    @Override
    public Set<String> getAllFilesInDir(String dir, String prefixFileName)
    {
        Set<String> allFilesWithPointedPrefix = new HashSet<>();
        try {
            this.printWorkingDirectory();
            @SuppressWarnings("rawtypes")
            Vector allFiles = this.channelSftp.ls(dir);
            LOG.debug("list files: {}", JSON.toJSONString(allFiles, JSONWriter.Feature.UseSingleQuotes));
            for (Object allFile : allFiles) {
                LsEntry le = (LsEntry) allFile;
                String strName = le.getFilename();
                if (strName.startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(strName);
                }
            }
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "failed to list files in " + dir, e);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete)
    {
        String eachFile = null;
        try {
            this.printWorkingDirectory();
            for (String each : filesToDelete) {
                LOG.info("delete file {}", each);
                eachFile = each;
                this.channelSftp.rm(each);
            }
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(
                    IO_ERROR, "failed to delete file " + eachFile, e);
        }
    }

    private void printWorkingDirectory()
    {
        try {
            LOG.info("current working directory {}", channelSftp.pwd());
        }
        catch (SftpException e) {
            LOG.error("failed to print current working directory", e);
        }
    }

    @Override
    public void completePendingCommand()
    {
    }
}
