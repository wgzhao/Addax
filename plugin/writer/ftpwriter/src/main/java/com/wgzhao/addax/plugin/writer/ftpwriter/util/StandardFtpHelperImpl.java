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
import com.wgzhao.addax.plugin.writer.ftpwriter.FtpConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static com.wgzhao.addax.core.spi.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.LOGIN_ERROR;
import static org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE;

/** Standard Ftp Helper Impl. */
public class StandardFtpHelperImpl
        implements IFtpHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelperImpl.class);

    // the client is replaced on every login attempt, so a retry never reuses a half-open one
    private FTPClient ftpClient;

    @Override
    public void loginFtpServer(FtpConnection connection)
    {
        this.ftpClient = new FTPClient();
        try {
            // the control connection's reader and writer are built while connecting, from the
            // encoding that is set at that moment, so it has to be in place before the socket opens
            this.ftpClient.setControlEncoding(StandardCharsets.UTF_8.name());
            this.ftpClient.setConnectTimeout(connection.timeout());
            this.ftpClient.setDefaultTimeout(connection.timeout());
            this.ftpClient.connect(connection.host(), connection.port());
            this.ftpClient.login(connection.username(), connection.password());
            // the timeout is configured in milliseconds, a Duration of seconds would let a
            // stalled transfer hang for hours
            this.ftpClient.setDataTimeout(Duration.ofMillis(connection.timeout()));
            if ("PORT".equalsIgnoreCase(connection.connectPattern())) {
                this.ftpClient.enterLocalActiveMode();
            }
            else {
                this.ftpClient.enterLocalPassiveMode();
            }
            int reply = this.ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                throw new IOException("the server rejected the login with reply code " + reply);
            }
            // always use binary transfer mode
            this.ftpClient.setFileType(BINARY_FILE_TYPE);
        }
        catch (Exception e) {
            closeQuietly();
            throw AddaxException.asAddaxException(LOGIN_ERROR, String.format(
                    "Failed to connect the ftp server %s:%s", connection.host(), connection.port()), e);
        }
    }

    @Override
    public void logoutFtpServer()
    {
        if (this.ftpClient == null) {
            return;
        }
        try {
            if (this.ftpClient.isConnected()) {
                this.ftpClient.logout();
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, "Failed to close the connection", e);
        }
        finally {
            // logout only ends the session, the control socket stays open until it is disconnected
            closeQuietly();
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
                    throw AddaxException.asAddaxException(EXECUTE_FAIL,
                            "Failed to create the directory " + dirPath);
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to create the directory " + directoryPath, e);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath)
    {
        try {
            // storing truncates a file the name already refers to; appending would concatenate
            // whatever a previous run left behind
            OutputStream writeOutputStream = this.ftpClient.storeFileStream(filePath);
            if (null == writeOutputStream) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to open the file for writing: " + filePath);
            }
            return writeOutputStream;
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to open the file for writing: " + filePath, e);
        }
    }

    @Override
    public boolean exists(String filePath)
    {
        try {
            // the ftp protocol has no stat command, and a listing of a path that is not there
            // comes back as an empty listing instead of an error
            return this.ftpClient.listFiles(filePath).length > 0;
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to check whether the path exists: " + filePath, e);
        }
    }

    @Override
    public void completePendingCommand()
    {
        try {
            // reading the transfer's reply keeps the control connection in sync; without it the
            // next command reads this reply and the listing comes back empty, and a transfer the
            // server rejected (quota, permission) would never be noticed
            if (!this.ftpClient.completePendingCommand()) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "the server did not accept the upload, reply: " + this.ftpClient.getReplyString().trim());
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to read the transfer confirmation", e);
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
            FTPFile[] fs = this.ftpClient.listFiles(dir);
            if (LOG.isDebugEnabled()) {
                LOG.debug("list files in {}: {}", dir, JSON.toJSONString(fs, JSONWriter.Feature.UseSingleQuotes));
            }
            for (FTPFile ff : fs) {
                // a sub directory whose name carries the prefix is no data file: deleting it or
                // conflicting on it would only fail the job
                if (!ff.isDirectory() && ff.getName().startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(ff.getName());
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to list the files in " + dir, e);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete)
    {
        try {
            for (String each : filesToDelete) {
                LOG.info("Try to delete file {}", each);
                if (!this.ftpClient.deleteFile(each)) {
                    throw AddaxException.asAddaxException(IO_ERROR,
                            "Failed to delete file " + each + ", please check the permission");
                }
            }
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to delete file", e);
        }
    }

    /**
     * Create one level of the directory tree.
     *
     * @param directoryPath the level to create
     * @return true when the level exists afterwards
     * @throws IOException when the server cannot be reached
     */
    private boolean mkDirSingleHierarchy(String directoryPath)
            throws IOException
    {
        boolean isDirExist = this.ftpClient.changeWorkingDirectory(directoryPath);
        if (isDirExist) {
            return true;
        }
        int replayCode = this.ftpClient.mkd(directoryPath);
        return replayCode == FTPReply.COMMAND_OK || replayCode == FTPReply.PATHNAME_CREATED;
    }

    /** Close the control connection, whatever state it is in. */
    private void closeQuietly()
    {
        if (this.ftpClient != null && this.ftpClient.isConnected()) {
            try {
                this.ftpClient.disconnect();
            }
            catch (IOException e) {
                LOG.error("Failed to close the connection", e);
            }
        }
        this.ftpClient = null;
    }
}
