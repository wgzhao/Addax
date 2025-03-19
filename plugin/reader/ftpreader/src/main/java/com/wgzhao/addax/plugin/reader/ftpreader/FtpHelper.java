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
    protected final HashSet<String> sourceFiles = new HashSet<>();

    public abstract void loginFtpServer(String host, String username, String password, int port, String keyPath, String keyPass, int timeout, String connectMode);

    public abstract void logoutFtpServer();

    /**
     * List files under the specified directory up to the maximum traversal level.
     * @param directoryPath Path to check for files
     * @param parentLevel Current traversal level
     * @param maxTraversalLevel Maximum depth to traverse
     */
    public abstract void getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel);

    /**
     * Get input stream for reading a file
     * @param filePath Path to the file
     * @return Input stream for the file
     */
    public abstract InputStream getInputStream(String filePath);

    /**
     * Check if the path contains wildcard characters
     * @param path Path to check
     * @return true if path contains wildcards
     */
    protected boolean hasWildcard(String path) {
        return path.contains("*") || path.contains("?");
    }

    /**
     * Match filename against a pattern with wildcards
     * @param pattern Pattern with possible wildcards (* and ?)
     * @param filename Filename to check
     * @return true if filename matches pattern
     */
    protected boolean matchWildcard(String pattern, String filename) {
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return filename.matches(regex);
    }

    /**
     * Get all files from a list of source paths
     * @param srcPaths List of paths to scan
     * @param parentLevel Initial level (usually 0)
     * @param maxTraversalLevel Maximum traversal depth
     * @return Set containing all found files
     */
    public Set<String> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel)
    {
        sourceFiles.clear(); // Clear previous results
        if (srcPaths != null && !srcPaths.isEmpty()) {
            for (String eachPath : srcPaths) {
                getListFiles(eachPath, parentLevel, maxTraversalLevel);
            }
        }
        return new HashSet<>(sourceFiles); // Return a copy to prevent modification
    }

    /**
     * Check if a path is a directory
     * @param directoryPath Path to check
     * @return true if it's a directory, false otherwise
     */
    protected abstract boolean isDirectory(String directoryPath);
}
