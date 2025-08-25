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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

/**
 * Utility class for file operations including compression detection,
 * file reading, and path manipulation.
 *
 * @author wgzhao
 * @since 1.0.0
 */
public final class FileHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(FileHelper.class);

    /**
     * Mapping of compression type names to their corresponding file suffixes
     */
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

    /**
     * Default buffer size for reading operations
     */
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    /**
     * Private constructor to prevent instantiation
     */
    private FileHelper()
    {
        // Utility class
    }

    /**
     * Detect the compression type of the specified file.
     *
     * @param fileName the file name to analyze
     * @return the compression type if present, otherwise "none"
     * @throws IOException if there's an error reading the file
     */
    public static String getFileCompressType(String fileName)
            throws IOException
    {
        if (StringUtils.isBlank(fileName)) {
            LOG.warn("Empty or null file name provided for compression detection");
            return "none";
        }

        LOG.debug("Detecting compression type for file: {}", fileName);

        Path filePath = Paths.get(fileName);
        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("File not found: " + fileName);
        }

        if (!Files.isReadable(filePath)) {
            throw new IOException("File is not readable: " + fileName);
        }

        try (InputStream inputStream = Files.newInputStream(filePath);
             BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
            return getFileCompressType(bufferedInputStream);
        }
        catch (IOException e) {
            LOG.error("Failed to detect compression type for file: {}", fileName, e);
            throw e;
        }
    }

    /**
     * Detect compression type from an input stream.
     * The input stream must support mark/reset operations.
     *
     * @param inputStream the input stream to detect (must support mark)
     * @return the compression type if present, otherwise "none"
     * @throws IllegalArgumentException if the input stream doesn't support mark
     */
    public static String getFileCompressType(InputStream inputStream)
    {
        if (inputStream == null) {
            throw new IllegalArgumentException("Input stream cannot be null");
        }

        if (!inputStream.markSupported()) {
            throw new IllegalArgumentException("Input stream must support mark operation");
        }

        try {
            String type = CompressorStreamFactory.detect(inputStream);
            LOG.debug("Detected compression type: {}", type);
            return type;
        }
        catch (CompressorException e) {
            LOG.debug("No compression detected, assuming uncompressed file");
            return "none";
        }
    }

    /**
     * Return the corresponding file name suffix according to compression algorithm.
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
     * Read a compressed file and return a buffered reader.
     *
     * @param fileName the file to read
     * @param encoding character encoding to use (if null, uses UTF-8)
     * @param bufferSize buffer size for reading (if &le; 0, uses default)
     * @return BufferedReader for reading the file content
     * @throws AddaxException if reading fails
     */
    public static BufferedReader readCompressFile(String fileName, String encoding, int bufferSize)
    {
        if (StringUtils.isBlank(fileName)) {
            throw new IllegalArgumentException("File name cannot be blank");
        }

        // Validate and set defaults
        String actualEncoding = StringUtils.isBlank(encoding) ? StandardCharsets.UTF_8.name() : encoding;
        int actualBufferSize = bufferSize <= 0 ? DEFAULT_BUFFER_SIZE : bufferSize;

        LOG.debug("Reading compressed file: {} with encoding: {}, buffer size: {}",
                fileName, actualEncoding, actualBufferSize);

        try {
            String compressType = getFileCompressType(fileName);
            LOG.debug("Detected compression type: {} for file: {}", compressType, fileName);
            Path path = Paths.get(fileName);

            return switch (compressType) {
                case "none" -> Files.newBufferedReader(path, Charset.forName(actualEncoding));
                case "zip" -> {
                    InputStream inputStream = Files.newInputStream(path);
                    ZipCycleInputStream zis = new ZipCycleInputStream(inputStream);
                    yield new BufferedReader(new InputStreamReader(zis, actualEncoding), actualBufferSize);
                }
                default -> {
                    InputStream inputStream = Files.newInputStream(path);
                    BufferedInputStream bis = new BufferedInputStream(inputStream);
                    InputStream compressedInput = new CompressorStreamFactory()
                            .createCompressorInputStream(compressType, bis, true);
                    yield new BufferedReader(new InputStreamReader(compressedInput, actualEncoding), actualBufferSize);
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
     * Build a list of source files from directories, handling wildcards.
     *
     * @param directories list of directory paths, possibly with wildcards
     * @return list of resolved file paths (never null)
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

            try {
                if (eachPath.contains("*") || eachPath.contains("?")) {
                    LOG.debug("Processing wildcard path: {}", eachPath);
                    List<String> matched = listFilesWithWildcard(eachPath);
                    LOG.debug("Found {} files matching wildcard path: {}", matched.size(), eachPath);
                    toBeReadFiles.addAll(matched);
                }
                else {
                    Path pathObj = Paths.get(eachPath);
                    if (Files.isDirectory(pathObj)) {
                        LOG.debug("Processing directory: {}", eachPath);
                        // Use proper path joining instead of string concatenation
                        String wildcardPath = pathObj.resolve("*.*").toString();
                        List<String> dirFiles = listFilesWithWildcard(wildcardPath);
                        LOG.debug("Found {} files in directory: {}", dirFiles.size(), eachPath);
                        toBeReadFiles.addAll(dirFiles);
                    }
                    else if (Files.exists(pathObj)) {
                        LOG.debug("Adding single file: {}", eachPath);
                        toBeReadFiles.add(pathObj.toAbsolutePath().toString());
                    }
                    else {
                        LOG.warn("Path does not exist: {}", eachPath);
                    }
                }
            }
            catch (Exception e) {
                LOG.error("Error processing path: {}", eachPath, e);
            }
        }

        List<String> result = new ArrayList<>(toBeReadFiles);
        LOG.info("Total source files to process: {}", result.size());
        return result;
    }

    /**
     * List files that match a wildcard pattern.
     *
     * @param wildcardPath path with wildcard patterns
     * @return list of matching files (never null)
     */
    private static List<String> listFilesWithWildcard(String wildcardPath)
    {
        List<String> result = new ArrayList<>();
        if (StringUtils.isBlank(wildcardPath)) {
            LOG.warn("Empty wildcard path provided");
            return result;
        }

        LOG.debug("Listing files with wildcard: {}", wildcardPath);

        try {
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
                    if (Files.isRegularFile(entry)) {
                        result.add(entry.toAbsolutePath().toString());
                    }
                }
                LOG.debug("Found {} files matching pattern: {}", result.size(), globPattern);
            }
        }
        catch (Exception e) {
            LOG.error("Failed to list files with wildcard path: {}", wildcardPath, e);
        }

        return result;
    }

    /**
     * Split a list into approximately equal-sized sublists.
     *
     * @param <T> the type of elements in the list
     * @param sourceList the list to split (can be null or empty)
     * @param adviceNumber suggested number of sublists (must be positive)
     * @return list of sublists (never null)
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

        // Optimize: calculate the actual number of splits needed
        int actualSplits = Math.min(adviceNumber, sourceList.size());
        int averageLength = sourceList.size() / actualSplits;
        int remainder = sourceList.size() % actualSplits;

        int startIndex = 0;
        for (int i = 0; i < actualSplits; i++) {
            int endIndex = startIndex + averageLength + (i < remainder ? 1 : 0);
            splitedList.add(new ArrayList<>(sourceList.subList(startIndex, endIndex)));
            startIndex = endIndex;
        }

        LOG.debug("Split into {} actual parts", splitedList.size());
        return splitedList;
    }

    /**
     * Generate a unique file name middle part based on timestamp and random characters.
     * Format: yyyyMMdd_HHmmss_SSS_randomChars
     *
     * @return generated string for file naming (never null)
     */
    public static String generateFileMiddleName()
    {
        String randomChars = "0123456789abcdefghmnpqrstuvwxyz";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
        // Generate format like: 20211203_143329_237_6587fddb
        return dateFormat.format(new Date()) + "_" +
               RandomStringUtils.insecure().next(8, randomChars);
    }
}
