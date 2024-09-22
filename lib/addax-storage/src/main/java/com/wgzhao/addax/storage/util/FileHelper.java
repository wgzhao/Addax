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

import com.google.common.collect.ImmutableMap;
import com.wgzhao.addax.common.compress.ZipCycleInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FileHelper {
    public final static Logger LOG = LoggerFactory.getLogger(FileHelper.class);

    private final static ImmutableMap<String, String> COMPRESS_TYPE_SUFFIX_MAP = new ImmutableMap.Builder<String, String>()
            .put("BZIP", ".bz2")
            .put("BZIP2", ".bz2")
            .put("DEFLATE", ".deflate")
            .put("DEFLATE64", ".deflate")
            .put("GZIP", ".gz")
            .put("GZ", ".gz")
            .put("LZ4", ".lz4")
            .put("LZ4-BLOCK", ".lz4")
            .put("LZ4-FRAMED", ".lz4")
            .put("LZO", ".lzo")
            .put("LZOP", ".lzo")
            .put("SNAPPY", ".snappy")
            .put("XZ", ".xz")
            .put("Z", ".z")
            .put("ZIP", ".zip")
            .put("ZLIB", ".zlib")
            .put("ZSTANDARD", ".zstd")
            .put("ZSTD", ".zstd")
            .build();

    public static final HashMap<String, String> mFileTypes = new HashMap<>();

    static {
        // compress type magic number
        mFileTypes.put("504B", "zip");// zip 压缩文件
        mFileTypes.put("5261", "rar");
        mFileTypes.put("1F8B", "gz");
        mFileTypes.put("1F9D", "z");// z, tar.z using Lempel-Ziv-Welch algorithm
        mFileTypes.put("1FA0", "z"); //z, tar.z using LZH algorithm
        mFileTypes.put("425A", "bz2");
        mFileTypes.put("377A", "7z");
        mFileTypes.put("FD37", "xz");
        mFileTypes.put("0422", "lz4");
        mFileTypes.put("7573", "tar");
    }

    /**
     * detect the specified file compression type
     *
     * @param fileName the file name
     * @return the compression type if present, otherwise "none"
     */
    public static String getFileCompressType(String fileName) {
        try {
            InputStream inputStream = new FileInputStream(FilenameUtils.getFullPath(fileName));
            String fileType = getFileCompressType(inputStream);
            inputStream.close();
            return fileType;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + fileName, e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to close file: ", e);
        }
    }

    public static String getFileCompressType(InputStream inputStream) {
        try {
            return CompressorStreamFactory.detect(inputStream);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("does not support mark", e);
        } catch (CompressorException e) {
            return "none";
        }
    }

    /**
     * Return the corresponding file name suffixes according to different compression algorithms
     *
     * @param compress the compression type name
     * @return the suffix if present, otherwise ""
     */
    public static String getCompressFileSuffix(String compress) {
        if (compress == null || compress.isEmpty() || "none".equalsIgnoreCase(compress)) {
            return "";
        }
        return COMPRESS_TYPE_SUFFIX_MAP.getOrDefault(compress.toUpperCase(), "." + compress.toLowerCase());
    }

    public static BufferedReader readCompressFile(String fileName, String encoding, int bufferSize) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(FilenameUtils.getFullPath(fileName));
        } catch (FileNotFoundException e) {
            // warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
            throw new RuntimeException("File not found: " + fileName, e);
        }
        try {
            String compressType = getFileCompressType(fileName);

            if ("none".equals(compressType)) {
                return new BufferedReader(new InputStreamReader(inputStream, encoding), bufferSize);
            }

            if ("zip".equals(compressType)) {
                ZipCycleInputStream zis = new ZipCycleInputStream(inputStream);
                return new BufferedReader(new InputStreamReader(zis, encoding), bufferSize);
            }

            BufferedInputStream bis = new BufferedInputStream(inputStream);
            CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(compressType, bis, true);
            return new BufferedReader(new InputStreamReader(input, encoding), bufferSize);
        } catch (CompressorException | IOException e) {
            throw new RuntimeException("Failed to read compress file", e);
        }
    }

    public static boolean checkDirectoryWritable(String directory) {
        Path path = Paths.get(directory);
        return Files.isWritable(path);
    }

    // validate the path, path must be an absolute path
    public static List<String> buildSourceTargets(List<String> directories) {
        // for each path
        Set<String> toBeReadFiles = new HashSet<>();
        for (String eachPath : directories) {
            if (StringUtils.isBlank(eachPath)) {
                continue;
            }
            if (eachPath.contains("*") || eachPath.contains("?")) {
                toBeReadFiles.addAll(listFilesWithWildcard(eachPath));
            } else {
                File file = new File(eachPath);
                if (file.isDirectory()) {
                    // list all files in current directory
                    toBeReadFiles.addAll(listFilesWithWildcard(eachPath+"/*.*"));
                } else {
                    toBeReadFiles.add(eachPath);
                }
            }
        }
        return new ArrayList<>(toBeReadFiles);
    }

    private static List<String> listFilesWithWildcard(String wildcardPath) {
        List<String> result = new ArrayList<>();
        Path path = Paths.get(wildcardPath);
        Path dir = path.getParent();
        String globPattern = path.getFileName().toString();
        if (dir == null) {
            LOG.error("Invalid wildcard path: {}", wildcardPath);
            return result;
        }
        if (!Files.exists(dir)) {
            LOG.error("Directory not exists: {}", dir);
            return result;
        }
        // check the read permission
        if (!Files.isReadable(dir)) {
            LOG.error("No read permission for directory: {}", dir);
            return result;
        }
        // List all files that match the wildcard path
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, globPattern)) {
            for (Path entry : stream) {
                result.add(entry.toFile().getAbsolutePath());
            }
        } catch (IOException e) {
            LOG.error("Failed to list files with wildcard path: {}", wildcardPath, e);
        }
        return result;
    }

    public static <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
        List<List<T>> splitedList = new ArrayList<>();
        int averageLength = sourceList.size() / adviceNumber;
        averageLength = averageLength == 0 ? 1 : averageLength;

        for (int begin = 0, end; begin < sourceList.size(); begin = end) {
            end = begin + averageLength;
            if (end > sourceList.size()) {
                end = sourceList.size();
            }
            splitedList.add(sourceList.subList(begin, end));
        }
        return splitedList;
    }

    public static String generateFileMiddleName() {
        String randomChars = "0123456789abcdefghmnpqrstuvwxyz";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
        // like 2021-12-03-14-33-29-237-6587fddb
        return dateFormat.format(new Date()) + "_" + RandomStringUtils.random(8, randomChars);
    }
}
