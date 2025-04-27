/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.storage.util;

import com.wgzhao.addax.core.compress.ZipCycleInputStream;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.spi.ErrorCode;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(FileHelper.class);

    private static final Map<String, String> COMPRESS_TYPE_SUFFIX_MAP = Map.ofEntries(
            Map.entry("BZIP", ".bz2"),
            Map.entry("BZIP2", ".bz2"),
            Map.entry("DEFLATE", ".deflate"),
            Map.entry("DEFLATE64", ".deflate"),
            Map.entry("GZIP", ".gz"),
            Map.entry("GZ", ".gz"),
            Map.entry("LZ4", ".lz4"),
            Map.entry("LZ4-BLOCK", ".lz4"),
            Map.entry("LZ4-FRAMED", ".lz4"),
            Map.entry("LZO", ".lzo"),
            Map.entry("LZOP", ".lzo"),
            Map.entry("SNAPPY", ".snappy"),
            Map.entry("XZ", ".xz"),
            Map.entry("Z", ".z"),
            Map.entry("ZIP", ".zip"),
            Map.entry("ZLIB", ".zlib"),
            Map.entry("ZSTANDARD", ".zstd"),
            Map.entry("ZSTD", ".zstd")
    );

    public static final Map<String, String> FILE_MAGIC_TYPES = Map.ofEntries(
            Map.entry("504B", "zip"),
            Map.entry("5261", "rar"),
            Map.entry("1F8B", "gz"),
            Map.entry("1F9D", "z"),
            Map.entry("1FA0", "z"),
            Map.entry("425A", "bz2"),
            Map.entry("377A", "7z"),
            Map.entry("FD37", "xz"),
            Map.entry("0422", "lz4"),
            Map.entry("7573", "tar")
    );

    private FileHelper()
    {
        // Prevent instantiation
    }

    /**
     * Detect the specified file compression type
     *
     * @param fileName the file name
     * @return the compression type if present, otherwise "none"
     * @throws IOException if there's an error reading the file
     */
    public static String getFileCompressType(String fileName)
            throws IOException
    {
        if (fileName == null || fileName.isEmpty()) {
            LOG.warn("Empty file name provided for compression detection");
            return "none";
        }

        LOG.debug("Detecting compression type for file: {}", fileName);
        try (InputStream inputStream = new FileInputStream(fileName)) {
            return getFileCompressType(inputStream);
        }
        catch (FileNotFoundException e) {
            LOG.error("Failed to find file for compression detection: {}", fileName);
            throw new IOException("File not found: " + fileName, e);
        }
    }

    /**
     * Detect compression type from an input stream
     *
     * @param inputStream the input stream to detect
     * @return the compression type if present, otherwise "none"
     */
    public static String getFileCompressType(InputStream inputStream)
    {
        try {
            String type = CompressorStreamFactory.detect(inputStream);
            LOG.debug("Detected compression type: {}", type);
            return type;
        }
        catch (IllegalArgumentException e) {
            LOG.warn("Cannot detect compression type: stream does not support mark", e);
            throw new IllegalArgumentException("Input stream does not support mark operation", e);
        }
        catch (CompressorException e) {
            LOG.debug("No compression detected, assuming uncompressed file");
            return "none";
        }
    }

    /**
     * Return the corresponding file name suffix according to compression algorithm
     *
     * @param compress the compression type name
     * @return the suffix if present, otherwise empty string
     */
    public static String getCompressFileSuffix(String compress)
    {
        if (StringUtils.isBlank(compress) || "none".equalsIgnoreCase(compress)) {
            return "";
        }
        String suffix = COMPRESS_TYPE_SUFFIX_MAP.getOrDefault(compress.toUpperCase(), "." + compress.toLowerCase());
        LOG.debug("Compression type '{}' maps to suffix '{}'", compress, suffix);
        return suffix;
    }

    /**
     * Read a compressed file and return a buffered reader
     *
     * @param fileName the file to read
     * @param encoding character encoding to use
     * @param bufferSize buffer size for reading
     * @return BufferedReader for reading the file content
     */
    public static BufferedReader readCompressFile(String fileName, String encoding, int bufferSize)
    {
        if (StringUtils.isBlank(fileName)) {
            throw new IllegalArgumentException("File name cannot be blank");
        }

        LOG.debug("Reading compressed file: {} with encoding: {}", fileName, encoding);

        try {

            var compressType = getFileCompressType(fileName);
            LOG.debug("Detected compression type: {} for file: {}", compressType, fileName);
            var path = Path.of(fileName);

            return switch (compressType) {
                case "none" -> Files.newBufferedReader(path, Charset.forName(encoding));
                case "zip" -> {
                    var inputStream = new FileInputStream(fileName);
                    var zis = new ZipCycleInputStream(inputStream);
                    yield new BufferedReader(new InputStreamReader(zis, encoding), bufferSize);
                }
                default -> {
                    var inputStream = new FileInputStream(fileName);
                    var bis = new BufferedInputStream(inputStream);
                    var input = new CompressorStreamFactory()
                            .createCompressorInputStream(compressType, bis, true);
                    yield new BufferedReader(new InputStreamReader(input, encoding), bufferSize);
                }
            };
        }
        catch (IOException | CompressorException e) {
            throw AddaxException.asAddaxException(
                    ErrorCode.IO_ERROR,
                    "Failed to read compressed file: " + fileName,
                    e
            );
        }
    }

    /**
     * Build a list of source files from directories, handling wildcards
     *
     * @param directories list of directory paths, possibly with wildcards
     * @return list of resolved file paths
     */
    public static List<String> buildSourceTargets(List<String> directories)
    {
        if (directories == null || directories.isEmpty()) {
            LOG.info("No directories specified for source targets");
            return new ArrayList<>();
        }

        LOG.debug("Building source targets from {} directories/paths", directories.size());
        Set<String> toBeReadFiles = new HashSet<>();

        for (String eachPath : directories) {
            if (StringUtils.isBlank(eachPath)) {
                LOG.debug("Skipping blank path entry");
                continue;
            }

            if (eachPath.contains("*") || eachPath.contains("?")) {
                LOG.debug("Processing wildcard path: {}", eachPath);
                List<String> matched = listFilesWithWildcard(eachPath);
                LOG.debug("Found {} files matching wildcard path: {}", matched.size(), eachPath);
                toBeReadFiles.addAll(matched);
            }
            else {
                File file = new File(eachPath);
                if (file.isDirectory()) {
                    LOG.debug("Processing directory: {}", eachPath);
                    List<String> dirFiles = listFilesWithWildcard(eachPath + "/*.*");
                    LOG.debug("Found {} files in directory: {}", dirFiles.size(), eachPath);
                    toBeReadFiles.addAll(dirFiles);
                }
                else {
                    LOG.debug("Adding single file: {}", eachPath);
                    toBeReadFiles.add(eachPath);
                }
            }
        }

        LOG.info("Total source files to process: {}", toBeReadFiles.size());
        return new ArrayList<>(toBeReadFiles);
    }

    /**
     * List files that match a wildcard pattern
     *
     * @param wildcardPath path with wildcard patterns
     * @return list of matching files
     */
    private static List<String> listFilesWithWildcard(String wildcardPath)
    {
        List<String> result = new ArrayList<>();
        if (StringUtils.isBlank(wildcardPath)) {
            LOG.warn("Empty wildcard path provided");
            return result;
        }

        LOG.debug("Listing files with wildcard: {}", wildcardPath);
        Path path = Paths.get(wildcardPath);
        Path dir = path.getParent();
        String globPattern = path.getFileName().toString();

        if (dir == null) {
            LOG.error("Invalid wildcard path (no parent directory): {}", wildcardPath);
            return result;
        }

        if (!Files.exists(dir)) {
            LOG.warn("Directory does not exist: {}", dir);
            return result;
        }

        if (!Files.isReadable(dir)) {
            LOG.error("No read permission for directory: {}", dir);
            return result;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, globPattern)) {
            for (Path entry : stream) {
                result.add(entry.toFile().getAbsolutePath());
            }
            LOG.debug("Found {} files matching pattern: {}", result.size(), globPattern);
        }
        catch (IOException e) {
            LOG.error("Failed to list files with wildcard path: {}", wildcardPath, e);
        }

        return result;
    }

    /**
     * Split a list into approximately equal-sized sublists
     *
     * @param sourceList the list to split
     * @param adviceNumber suggested number of sublists
     * @return list of sublists
     */
    public static <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber)
    {
        List<List<T>> splitedList = new ArrayList<>();

        if (sourceList == null || sourceList.isEmpty()) {
            LOG.debug("Empty source list, returning empty result");
            return splitedList;
        }

        if (adviceNumber <= 0) {
            LOG.warn("Invalid advice number: {}, using 1 instead", adviceNumber);
            adviceNumber = 1;
        }

        LOG.debug("Splitting {} items into approximately {} parts", sourceList.size(), adviceNumber);
        int averageLength = Math.max(1, sourceList.size() / adviceNumber);

        for (int begin = 0, end; begin < sourceList.size(); begin = end) {
            end = Math.min(begin + averageLength, sourceList.size());
            splitedList.add(sourceList.subList(begin, end));
        }

        return splitedList;
    }

    /**
     * Generate a unique file name middle part based on timestamp and random chars
     *
     * @return generated string for file naming
     */
    public static String generateFileMiddleName()
    {
        String randomChars = "0123456789abcdefghmnpqrstuvwxyz";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
        // like 20211203_143329_237_6587fddb
        return dateFormat.format(new Date()) + "_" + RandomStringUtils.insecure().next(8, randomChars);
    }
}