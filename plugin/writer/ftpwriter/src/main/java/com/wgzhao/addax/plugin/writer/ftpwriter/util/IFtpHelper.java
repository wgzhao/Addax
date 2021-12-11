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

import java.io.OutputStream;
import java.util.Set;

public interface IFtpHelper
{

    /**
     * @param host the host to connect to
     * @param port the port to connect
     * @param username username to connect
     * @param password password for username
     * @param keyPath private key, only for sftp protocol
     * @param keyPass the passphrase of private key
     * @param timeout to connect timeout microseconds
     */
    void loginFtpServer(String host, int port, String username, String password, String keyPath, String keyPass, int timeout);

    void logoutFtpServer();

    /**
     * warn: 不支持递归创建, 比如 mkdir -p
     *
     * @param directoryPath the path
     */
    void mkdir(String directoryPath);

    /**
     * 支持目录递归创建
     *
     * @param directoryPath the path
     */
    void mkDirRecursive(String directoryPath);

    OutputStream getOutputStream(String filePath);

    String getRemoteFileContent(String filePath);

    Set<String> getAllFilesInDir(String dir, String prefixFileName);

    /**
     * warn: 不支持文件夹删除, 比如 rm -rf
     *
     * @param filesToDelete list of files which to be deleted
     */
    void deleteFiles(Set<String> filesToDelete);

    void completePendingCommand();
}
