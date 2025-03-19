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
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static com.wgzhao.addax.common.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;

public class StandardFtpHelper
        extends FtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelper.class);
    FTPClient ftpClient = null;

    @Override
    public void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout,
            String connectMode)
    {
        ftpClient = new FTPClient();
        try {
            ftpClient.connect(host, port);
            ftpClient.login(username, password);
            ftpClient.setConnectTimeout(timeout);
            ftpClient.setDataTimeout(Duration.ofMillis(timeout));
            if ("PASV".equals(connectMode)) {
                ftpClient.enterRemotePassiveMode();
                ftpClient.enterLocalPassiveMode();
            }
            else if ("PORT".equals(connectMode)) {
                ftpClient.enterLocalActiveMode();
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                throw AddaxException.asAddaxException(CONNECT_ERROR,
                        "Failed to connect to the ftp server " + host);
            }
            String fileEncoding = Charset.defaultCharset().displayName();
            ftpClient.setControlEncoding(fileEncoding);
            // always use binary transfer model
            ftpClient.setFileType(BINARY_FILE_TYPE);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Failed to connect to the ftp server " + host, e);
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (ftpClient.isConnected()) {
            try {
                ftpClient.logout();
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR,
                        "Failed to close the connection", e);
            }
        }
    }

    @Override
    protected boolean isDirectory(String directoryPath)
    {
        try {
            return ftpClient.changeWorkingDirectory(directoryPath);
        }
        catch (IOException e) {
            LOG.error("Failed to check whether the directory exists", e);
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

                if (!isDirectory(parentDir)) {
                    LOG.warn("Parent directory does not exist: {}", parentDir);
                    return;
                }

                FTPFile[] ftpFiles = ftpClient.listFiles(parentDir);
                for (FTPFile ftpFile : ftpFiles) {
                    if (ftpFile.isFile() && matchWildcard(filePattern, ftpFile.getName())) {
                        String filePath = parentDir + "/" + ftpFile.getName();
                        sourceFiles.add(filePath);
                        LOG.debug("Added file (wildcard match): {}", filePath);
                    }
                }
                return;
            }

            // Regular path handling
            if (!isDirectory(directoryPath)) {
                // If it's a file and exists, add it directly
                if (ftpClient.listFiles(directoryPath).length > 0) {
                    sourceFiles.add(directoryPath);
                    LOG.debug("Added file: {}", directoryPath);
                }
                return;
            }

            // Ensure directory path ends with separator
            String normalizedPath = directoryPath.endsWith(IOUtils.DIR_SEPARATOR + "") ?
                    directoryPath : directoryPath + IOUtils.DIR_SEPARATOR;

            FTPFile[] ftpFiles = ftpClient.listFiles(directoryPath);
            if (ftpFiles == null || ftpFiles.length == 0) {
                LOG.info("No files found in directory: {}", directoryPath);
                return;
            }

            for (FTPFile ftpFile : ftpFiles) {
                String fileName = ftpFile.getName();
                // Skip current directory and parent directory entries
                if (".".equals(fileName) || "..".equals(fileName)) {
                    continue;
                }

                String fullPath = normalizedPath + fileName;

                if (ftpFile.isFile()) {
                    sourceFiles.add(fullPath);
                    LOG.debug("Added file: {}", fullPath);
                }
                else if (ftpFile.isDirectory()) {
                    // Recursively traverse subdirectories
                    getListFiles(fullPath, parentLevel + 1, maxTraversalLevel);
                }
            }
        }
        catch (IOException e) {
            LOG.error("Failed to retrieve files from {}: {}", directoryPath, e.getMessage());
        }
    }

    @Override
    public InputStream getInputStream(String filePath)
    {
        try {
            InputStream inputStream = ftpClient.retrieveFileStream(
                    new String(filePath.getBytes(), StandardCharsets.ISO_8859_1));
            if (inputStream == null) {
                throw new IOException("Could not open stream for file: " + filePath);
            }

            // Ensure FTP command is completed after stream is closed
            return new FilterInputStream(inputStream)
            {
                @Override
                public void close()
                        throws IOException
                {
                    try {
                        super.close();
                    }
                    finally {
                        if (!ftpClient.completePendingCommand()) {
                            LOG.warn("Failed to complete pending command for file: {}", filePath);
                        }
                    }
                }
            };
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to read the file: " + filePath, e);
        }
    }
}
