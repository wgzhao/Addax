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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Properties;
import java.util.Vector;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.RUNTIME_ERROR;

/** Sftp Helper. */
public class SftpHelper
        extends FtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(SftpHelper.class);

    Session session = null;
    ChannelSftp channelSftp = null;

    @Override
    public void loginFtpServer(FtpConnection connection)
    {
        JSch jsch = new JSch(); // 创建JSch对象
        if (connection.keyPath() != null) {
            try {
                if (connection.keyPass() != null) {
                    jsch.addIdentity(connection.keyPath(), connection.keyPass());
                }
                else {
                    jsch.addIdentity(connection.keyPath());
                }
            }
            catch (JSchException e) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "Failed to use private key", e);
            }
        }
        try {
            session = jsch.getSession(connection.username(), connection.host(), connection.port());
            if (session == null) {
                throw AddaxException.asAddaxException(CONNECT_ERROR,
                        "Failed to connect server " + connection.host() + ":" + connection.port()
                                + " with user " + connection.username());
            }

            if (!StringUtils.isBlank(connection.password())) {
                session.setPassword(connection.password());
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.setTimeout(connection.timeout());
            // setTimeout only covers reads and writes, the tcp connect needs its own timeout
            session.connect(connection.timeout());

            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
        }
        catch (JSchException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Failed to connect server " + connection.host() + ":" + connection.port()
                            + " with user " + connection.username(), e
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
                String parentDir = parentDirOf(directoryPath);
                String filePattern = directoryPath.substring(directoryPath.lastIndexOf('/') + 1);

                try {
                    if (!isDirectory(parentDir)) {
                        LOG.warn("The directory [{}] of the pattern [{}] does not exist or is not readable",
                                parentDir, directoryPath);
                        return;
                    }

                    PathMatcher matcher = compileWildcard(filePattern);
                    Vector<LsEntry> vector = channelSftp.ls(parentDir);
                    int matched = 0;
                    for (LsEntry entry : vector) {
                        String fileName = entry.getFilename();
                        if (!".".equals(fileName) && !"..".equals(fileName) &&
                                !entry.getAttrs().isDir() && matcher.matches(Path.of(fileName))) {
                            String filePath = parentDir + "/" + fileName;
                            sourceFiles.add(filePath);
                            matched++;
                            LOG.debug("Added file (wildcard match): {}", filePath);
                        }
                    }
                    if (matched == 0) {
                        LOG.warn("No file under [{}] matches the pattern [{}]", parentDir, filePattern);
                    }
                }
                catch (SftpException e) {
                    LOG.error("Failed to list directory with wildcard: {}", parentDir, e);
                }
                return;
            }

            // Regular path handling
            if (isDirectory(directoryPath)) {
                listDirectory(directoryPath, parentLevel, maxTraversalLevel);
                return;
            }

            // Not a directory: it may still be a single file
            try {
                channelSftp.lstat(directoryPath);
                sourceFiles.add(directoryPath);
                LOG.debug("Added file: {}", directoryPath);
            }
            catch (SftpException e) {
                LOG.warn("The path [{}] does not exist or is not readable, it is skipped", directoryPath);
            }
        }
        catch (SftpException e) {
            LOG.error("Failed to retrieve files from {}: {}", directoryPath, e.getMessage());
        }
    }

    /**
     * List one directory and recurse into its subdirectories. The caller must already know that
     * the path is a directory, so that a directory found in a listing does not cost another
     * round trip to be recognized.
     */
    private void listDirectory(String directoryPath, int parentLevel, int maxTraversalLevel)
            throws SftpException
    {
        // sftp paths are always separated with '/', whatever the local platform uses
        String normalizedPath = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";

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
                if (parentLevel + 1 <= maxTraversalLevel) {
                    // Recursively traverse subdirectories
                    listDirectory(fullPath, parentLevel + 1, maxTraversalLevel);
                }
            }
            else if (attrs.isReg()) {
                sourceFiles.add(fullPath);
                LOG.debug("Added file: {}", fullPath);
            }
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
