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
import com.wgzhao.addax.plugin.writer.ftpwriter.FtpConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;

/** Sftp Helper Impl. */
public class SftpHelperImpl
        implements IFtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelperImpl.class);

    private Session session = null;
    private ChannelSftp channelSftp = null;

    @Override
    public void loginFtpServer(FtpConnection connection)
    {
        JSch jsch = new JSch();
        if (connection.keyPath() != null) {
            try {
                if (connection.keyPass() != null) {
                    jsch.addIdentity(connection.keyPath(), connection.keyPass().getBytes(StandardCharsets.UTF_8));
                }
                else {
                    jsch.addIdentity(connection.keyPath());
                }
            }
            catch (JSchException e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "Failed to load the private key " + connection.keyPath(), e);
            }
        }
        try {
            this.session = jsch.getSession(connection.username(), connection.host(), connection.port());
            if (connection.password() != null) {
                this.session.setPassword(connection.password().getBytes(StandardCharsets.UTF_8));
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            this.session.setConfig(config);
            this.session.setTimeout(connection.timeout());
            this.session.connect(connection.timeout());
            this.channelSftp = (ChannelSftp) this.session.openChannel("sftp");
            this.channelSftp.connect();
        }
        catch (JSchException e) {
            // the session may already have connected when the channel failed, do not leak it
            logoutFtpServer();
            String message = String.format("Failed to connect %s:%s because: %s",
                    connection.host(), connection.port(), e.getMessage());
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
    public void mkDirRecursive(String directoryPath)
    {
        SftpATTRS attrs = statOrNull(directoryPath);
        if (attrs != null && (attrs.isDir() || attrs.isLink())) {
            return;
        }
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
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to create the directory " + directoryPath, e);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath)
    {
        try {
            // overwriting truncates a file the name already refers to; appending would
            // concatenate whatever a previous run left behind
            OutputStream writeOutputStream = this.channelSftp.put(filePath, ChannelSftp.OVERWRITE);
            if (null == writeOutputStream) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to open the file for writing: " + filePath);
            }
            return writeOutputStream;
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to open the file for writing: " + filePath, e);
        }
    }

    @Override
    public boolean exists(String filePath)
    {
        return statOrNull(filePath) != null;
    }

    @Override
    public void completePendingCommand()
    {
        // sftp acknowledges each request in band, there is no reply left to read
    }

    @Override
    public Set<String> getAllFilesInDir(String dir, String prefixFileName)
    {
        Set<String> allFilesWithPointedPrefix = new HashSet<>();
        try {
            Vector<LsEntry> allFiles = this.channelSftp.ls(dir);
            if (LOG.isDebugEnabled()) {
                LOG.debug("list files in {}: {}", dir, JSON.toJSONString(allFiles, JSONWriter.Feature.UseSingleQuotes));
            }
            for (LsEntry each : allFiles) {
                // a sub directory whose name carries the prefix is no data file: deleting it or
                // conflicting on it would only fail the job
                if (!each.getAttrs().isDir() && each.getFilename().startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(each.getFilename());
                }
            }
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to list the files in " + dir, e);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete)
    {
        String eachFile = null;
        try {
            for (String each : filesToDelete) {
                LOG.info("delete file {}", each);
                eachFile = each;
                this.channelSftp.rm(each);
            }
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to delete file " + eachFile, e);
        }
    }

    /**
     * Create one level of the directory tree, the parent level is expected to exist already.
     *
     * @param directoryPath the level to create
     * @throws SftpException when the level cannot be created
     */
    private void mkDirSingleHierarchy(String directoryPath)
            throws SftpException
    {
        SftpATTRS attrs = statOrNull(directoryPath);
        if (attrs == null) {
            LOG.info("creating folder {}", directoryPath);
            this.channelSftp.mkdir(directoryPath);
        }
        else if (!attrs.isDir() && !attrs.isLink()) {
            // a regular file blocks the path, mkdir would only answer with a bare "Failure"
            throw new SftpException(ChannelSftp.SSH_FX_FAILURE,
                    "the path " + directoryPath + " exists but is not a directory");
        }
    }

    /**
     * Stat a path.
     *
     * @param filePath the path to stat
     * @return the attributes, or null when the path is not there
     */
    private SftpATTRS statOrNull(String filePath)
    {
        try {
            return this.channelSftp.lstat(filePath);
        }
        catch (SftpException e) {
            return null;
        }
    }
}
