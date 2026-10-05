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

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/** Ftp Helper. */
public abstract class FtpHelper
{
    protected final Set<String> sourceFiles = new HashSet<>();

    /**
     * Log in to the ftp server.
     * @param connection the connection settings
     */
    public abstract void loginFtpServer(FtpConnection connection);

    /** Logoutftpserver. */
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
     * Compile a pattern with wildcards into a matcher.
     * <p>
     * The pattern is compiled once per directory listing instead of once per file name, and the
     * JDK glob syntax keeps the characters that are regex operators (+ ( ) ^ $ |) literal, which
     * the hand-rolled regex conversion did not: the pattern {@code /a+b*.csv} used to match
     * {@code aab.csv} but not the file {@code a+b.csv} itself.
     *
     * @param pattern Pattern with possible wildcards (* and ?), only the last path segment
     * @return matcher to test file names against
     */
    protected PathMatcher compileWildcard(String pattern)
    {
        try {
            return FileSystems.getDefault().getPathMatcher("glob:" + pattern);
        }
        catch (PatternSyntaxException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The path pattern [" + pattern + "] is illegal", e);
        }
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
     * Take the directory part of a wildcard path. The pattern {@code /*.csv} lives in the root:
     * its parent is {@code /}, and no server accepts an empty pathname where the root is meant.
     *
     * @param path Path that carries a wildcard in its last segment
     * @return the directory the pattern has to be resolved in
     */
    protected String parentDirOf(String path)
    {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash <= 0 ? "/" : path.substring(0, lastSlash);
    }

    /**
     * Check if a path is a directory
     * @param directoryPath Path to check
     * @return true if it's a directory, false otherwise
     */
    protected abstract boolean isDirectory(String directoryPath);
}
