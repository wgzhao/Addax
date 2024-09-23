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

import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class FtpHelper
{
    /**
     * 与ftp服务器建立连接
     *
     * @param host 要连接的主机名，可以是IP地址
     * @param username 连接的账号
     * @param password 账号对应的密码
     * @param port 连接端口
     * @param timeout 超时时间（单位为秒）
     * @param connectMode 连接模式
     */
    public abstract void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout, String connectMode);

    /**
     * 断开与ftp服务器的连接
     */
    public abstract void logoutFtpServer();

    /**
     * 递归获取指定路径下符合条件的所有文件绝对路径
     *
     * @param directoryPath 根目录
     * @param parentLevel 父目录的递归层数（首次为0）
     * @param maxTraversalLevel 允许的最大递归层数
     * @return HashSet of String
     */
    public abstract Set<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel);

    /**
     * 获取指定路径的输入流
     *
     * @param filePath 需要获取的文件目录
     * @return InputStream
     */
    public abstract InputStream getInputStream(String filePath);

    /**
     * 获取指定路径列表下符合条件的所有文件的绝对路径
     *
     * @param srcPaths 路径列表
     * @param parentLevel 父目录的递归层数（首次为0）
     * @param maxTraversalLevel 允许的最大递归层数
     * @return HashSet of String
     */
    public Set<String> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel)
    {
        HashSet<String> sourceAllFiles = new HashSet<>();
        if (!srcPaths.isEmpty()) {
            for (String eachPath : srcPaths) {
                sourceAllFiles.addAll(getListFiles(eachPath, parentLevel, maxTraversalLevel));
            }
        }
        return sourceAllFiles;
    }
}
