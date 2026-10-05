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
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Duration;

import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;

/** Standard Ftp Helper. */
public class StandardFtpHelper
        extends FtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelper.class);
    FTPClient ftpClient = null;

    @Override
    public void loginFtpServer(FtpConnection connection)
    {
        ftpClient = new FTPClient();
        try {
            // The control connection's reader and writer are built while connecting, from the
            // encoding that is set at that moment, so setting it afterwards leaves the commands
            // on the default ISO-8859-1 and every non-ASCII path goes out as '?'
            ftpClient.setControlEncoding(StandardCharsets.UTF_8.name());
            // connectTimeout is read by connect() and the default timeout becomes the control
            // socket's SO_TIMEOUT, so both have to be in place before the socket is opened
            ftpClient.setConnectTimeout(connection.timeout());
            ftpClient.setDefaultTimeout(connection.timeout());
            ftpClient.connect(connection.host(), connection.port());
            ftpClient.login(connection.username(), connection.password());
            ftpClient.setDataTimeout(Duration.ofMillis(connection.timeout()));
            if ("PASV".equals(connection.connectPattern())) {
                ftpClient.enterRemotePassiveMode();
                ftpClient.enterLocalPassiveMode();
            }
            else if ("PORT".equals(connection.connectPattern())) {
                ftpClient.enterLocalActiveMode();
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                throw AddaxException.asAddaxException(CONNECT_ERROR,
                        "Failed to connect to the ftp server " + connection.host());
            }
            // always use binary transfer model
            ftpClient.setFileType(BINARY_FILE_TYPE);
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR,
                    "Failed to connect to the ftp server " + connection.host(), e);
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
            finally {
                // logout only ends the session, the control socket stays open until it is
                // disconnected
                if (ftpClient.isConnected()) {
                    try {
                        ftpClient.disconnect();
                    }
                    catch (IOException e) {
                        LOG.error("Failed to close the connection", e);
                    }
                }
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
                String parentDir = parentDirOf(directoryPath);
                String filePattern = directoryPath.substring(directoryPath.lastIndexOf('/') + 1);

                if (!isDirectory(parentDir)) {
                    LOG.warn("The directory [{}] of the pattern [{}] does not exist or is not readable",
                            parentDir, directoryPath);
                    return;
                }

                PathMatcher matcher = compileWildcard(filePattern);
                FTPFile[] ftpFiles = ftpClient.listFiles(parentDir);
                int matched = 0;
                for (FTPFile ftpFile : ftpFiles) {
                    if (ftpFile.isFile() && matcher.matches(Path.of(ftpFile.getName()))) {
                        String filePath = parentDir + "/" + ftpFile.getName();
                        sourceFiles.add(filePath);
                        matched++;
                        LOG.debug("Added file (wildcard match): {}", filePath);
                    }
                }
                if (matched == 0) {
                    LOG.warn("No file under [{}] matches the pattern [{}]", parentDir, filePattern);
                }
                return;
            }

            // Regular path handling
            if (isDirectory(directoryPath)) {
                listDirectory(directoryPath, parentLevel, maxTraversalLevel);
                return;
            }

            // Not a directory: it may still be a single file, which a listing confirms
            if (ftpClient.listFiles(directoryPath).length > 0) {
                sourceFiles.add(directoryPath);
                LOG.debug("Added file: {}", directoryPath);
            }
            else {
                LOG.warn("The path [{}] does not exist or is not readable, it is skipped", directoryPath);
            }
        }
        catch (IOException e) {
            LOG.error("Failed to retrieve files from {}: {}", directoryPath, e.getMessage());
        }
    }

    /**
     * List one directory and recurse into its subdirectories. The caller must already know that
     * the path is a directory: checking that on ftp costs a CWD round trip which also moves the
     * session's working directory, so entries that came out of a listing are not probed again.
     */
    private void listDirectory(String directoryPath, int parentLevel, int maxTraversalLevel)
            throws IOException
    {
        // ftp paths are always separated with '/', whatever the local platform uses
        String normalizedPath = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";

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
            else if (ftpFile.isDirectory() && parentLevel + 1 <= maxTraversalLevel) {
                // Recursively traverse subdirectories
                listDirectory(fullPath, parentLevel + 1, maxTraversalLevel);
            }
        }
    }

    @Override
    public InputStream getInputStream(String filePath)
    {
        try {
            InputStream inputStream = ftpClient.retrieveFileStream(filePath);
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
