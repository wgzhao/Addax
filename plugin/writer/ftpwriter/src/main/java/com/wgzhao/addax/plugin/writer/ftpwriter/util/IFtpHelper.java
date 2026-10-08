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

import com.wgzhao.addax.plugin.writer.ftpwriter.FtpConnection;

import java.io.OutputStream;
import java.util.Set;

/** IFtp Helper. */
public interface IFtpHelper
{
    /**
     * Log in to the server.
     *
     * @param connection the connection settings
     */
    void loginFtpServer(FtpConnection connection);

    /** Log out and close the connection, it is safe to call without a connection. */
    void logoutFtpServer();

    /**
     * Create the directory and every missing level above it.
     *
     * @param directoryPath the absolute directory path
     */
    void mkDirRecursive(String directoryPath);

    /**
     * Open the file for writing, an existing file of that name is truncated.
     *
     * @param filePath the absolute file path
     * @return the stream to write into
     */
    OutputStream getOutputStream(String filePath);

    /**
     * Read the server's confirmation of the last transfer. Ftp answers with a reply that has to
     * be read before the connection may be used again, sftp transfers are acknowledged in band
     * and this is a no-op there.
     */
    void completePendingCommand();

    /**
     * Whether the path exists on the server.
     *
     * @param filePath the absolute file path
     * @return true when the path exists
     */
    boolean exists(String filePath);

    /**
     * List the names of the entries in the directory whose name starts with the prefix.
     *
     * @param dir the absolute directory path
     * @param prefixFileName the name prefix to keep
     * @return the matching names
     */
    Set<String> getAllFilesInDir(String dir, String prefixFileName);

    /**
     * Delete the files.
     *
     * @param filesToDelete the absolute paths to delete
     */
    void deleteFiles(Set<String> filesToDelete);
}
