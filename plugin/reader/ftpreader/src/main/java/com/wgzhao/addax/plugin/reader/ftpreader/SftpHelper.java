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

package com.wgzhao.addax.plugin.reader.ftpreader;

import com.wgzhao.addax.core.exception.AddaxException;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

public class SftpHelper
        extends FtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelper.class);

    Session session = null;
    ChannelSftp channelSftp = null;

    @Override
    public void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout,
            String connectMode)
    {
        JSch jsch = new JSch(); // 创建JSch对象
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
                throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to use private key", e);
            }
        }
        try {
            session = jsch.getSession(username, host, port);
            if (session == null) {
                throw AddaxException.asAddaxException(CONNECT_ERROR,
                        "Failed to connect server " + host + ":" + port + " with user " + username);
            }

            if (!StringUtils.isBlank(password)) {
                session.setPassword(password);
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.setTimeout(timeout);
            session.connect();

            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
        }
        catch (JSchException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Failed to connect server " + host + ":" + port + " with user " + username, e
            );
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    @Override
    protected boolean isDirectory(String directoryPath)
    {
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(directoryPath);
            return sftpATTRS.isDir();
        }
        catch (SftpException e) {
            return false;
        }
    }

    @Override
    public void getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel)
    {
        if (parentLevel > maxTraversalLevel) {
            return;
        }

        try {
            // Handle wildcard pattern in the path
            if (hasWildcard(directoryPath)) {
                String parentDir = directoryPath.substring(0, directoryPath.lastIndexOf('/'));
                String filePattern = directoryPath.substring(directoryPath.lastIndexOf('/') + 1);

                try {
                    if (!isDirectory(parentDir)) {
                        LOG.warn("Parent directory does not exist: {}", parentDir);
                        return;
                    }

                    Vector<LsEntry> vector = channelSftp.ls(parentDir);
                    for (LsEntry entry : vector) {
                        String fileName = entry.getFilename();
                        if (!".".equals(fileName) && !"..".equals(fileName) &&
                                !entry.getAttrs().isDir() && matchWildcard(filePattern, fileName)) {
                            String filePath = parentDir + "/" + fileName;
                            sourceFiles.add(filePath);
                            LOG.debug("Added file (wildcard match): {}", filePath);
                        }
                    }
                }
                catch (SftpException e) {
                    LOG.error("Failed to list directory with wildcard: {}", parentDir, e);
                }
                return;
            }

            // Regular path handling
            if (!isDirectory(directoryPath)) {
                // Check if file exists
                try {
                    channelSftp.lstat(directoryPath);
                    sourceFiles.add(directoryPath);
                    LOG.debug("Added file: {}", directoryPath);
                }
                catch (SftpException e) {
                    LOG.warn("File does not exist: {}", directoryPath);
                }
                return;
            }

            // Ensure directory path ends with separator
            String normalizedPath = directoryPath.endsWith(IOUtils.DIR_SEPARATOR + "") ?
                    directoryPath : directoryPath + IOUtils.DIR_SEPARATOR;

            Vector<LsEntry> vector = channelSftp.ls(directoryPath);
            if (vector == null || vector.isEmpty()) {
                LOG.info("No files found in directory: {}", directoryPath);
                return;
            }

            for (LsEntry entry : vector) {
                String fileName = entry.getFilename();
                // Skip current directory and parent directory entries
                if (".".equals(fileName) || "..".equals(fileName)) {
                    continue;
                }

                String fullPath = normalizedPath + fileName;
                SftpATTRS attrs = entry.getAttrs();

                if (attrs.isDir()) {
                    // Recursively traverse subdirectories
                    getListFiles(fullPath, parentLevel + 1, maxTraversalLevel);
                }
                else if (attrs.isReg()) {
                    sourceFiles.add(fullPath);
                    LOG.debug("Added file: {}", fullPath);
                }
            }
        }
        catch (SftpException e) {
            LOG.error("Failed to retrieve files from {}: {}", directoryPath, e.getMessage());
        }
    }

    @Override
    public InputStream getInputStream(String filePath)
    {
        try {
            return channelSftp.get(filePath);
        }
        catch (SftpException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR,
                    "Failed to read file: " + filePath, e);
        }
    }
}
