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

    public abstract void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout, String connectMode);

    public abstract void logoutFtpServer();

    public abstract Set<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel);

    public abstract InputStream getInputStream(String filePath);

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
