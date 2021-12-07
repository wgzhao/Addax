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
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.compress.ZipCycleInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FileHelper
{
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
    public static String getFileCompressType(String fileName)
    {
        try {
            InputStream inputStream = new FileInputStream(fileName);
            return getFileCompressType(inputStream);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + fileName, e);
        }
    }

    public static String getFileCompressType(InputStream inputStream)
    {
        try {
            return CompressorStreamFactory.detect(inputStream);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException("does not support mark", e);
        }
        catch (CompressorException e) {
            return "none";
        }
    }

    /**
     * Return the corresponding file name suffixes according to different compression algorithms
     *
     * @param compress the compression type name
     * @return the suffix if present, otherwise ""
     */
    public static String getCompressFileSuffix(String compress)
    {
        if (compress == null || compress.isEmpty() || "none".equalsIgnoreCase(compress)) {
            return "";
        }
        return COMPRESS_TYPE_SUFFIX_MAP.getOrDefault(compress.toUpperCase(), "." + compress.toLowerCase());
    }

    public static BufferedReader readCompressFile(String fileName, String encoding)
    {
        return readCompressFile(fileName, encoding, Constant.DEFAULT_BUFFER_SIZE);
    }

    public static BufferedReader readCompressFile(String fileName, String encoding, int bufferSize)
    {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(fileName);
        }
        catch (FileNotFoundException e) {
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
        }
        catch (CompressorException | IOException e) {
            throw new RuntimeException("read compress file error", e);
        }
    }

    public static boolean checkDirectoryReadable(String directory)
    {
        return checkDirPermission(directory, "r");
    }

    public static boolean checkFileReadable(String fileName)
    {
        return checkFilePermission(fileName, "r");
    }

    public static boolean checkDirectoryWritable(String directory)
    {
        return checkDirPermission(directory, "w");
    }

    public static boolean checkFileWritable(String fileName)
    {
        return checkFilePermission(fileName, "w");
    }

    private static boolean checkFilePermission(String fileName, String permission)
    {
        File file = new File(fileName);
        if (!file.exists()) {
            throw new RuntimeException("file not exists: " + fileName);
        }
        if (!file.isFile()) {
            throw new RuntimeException("not a file: " + fileName);
        }

        if ("r".equalsIgnoreCase(permission)) {
            return file.canRead();
        }
        else {
            return file.canWrite();
        }
    }

    private static boolean checkDirPermission(String directory, String permission)
    {
        File file = new File(directory);
        if (!file.exists()) {
            throw new RuntimeException("directory not exists: " + directory);
        }
        if (!file.isDirectory()) {
            throw new RuntimeException("not a directory: " + directory);
        }

        if ("r".equalsIgnoreCase(permission)) {
            return file.canRead();
        }
        else {
            return file.canWrite();
        }
    }

    public static List<String> getAllFiles(String directory)
    {
        File file = new File(directory);
        if (!file.exists()) {
            return Collections.emptyList();
        }
        if (!file.isDirectory()) {
            return Collections.emptyList();
        }
        return Arrays.asList(file.list());
    }

    public static List<String> getAllFiles(List<String> directories)
    {
        List<String> files = new ArrayList<>();
        for (String directory : directories) {
            if (checkDirectoryReadable(directory)) {
                files.addAll(getAllFiles(directory));
            }
        }
        return files;
    }

    public static Pattern generatePattern(String dir)
    {
        String regexString = dir.replace("*", ".*").replace("?", ".?");
        return Pattern.compile(regexString);
    }

    public static boolean isTargetFile(Map<String, Pattern> patterns, Map<String, Boolean> isRegexPath, String regexPath, String absoluteFilePath)
    {
        if (isRegexPath.get(regexPath)) {
            return patterns.get(regexPath).matcher(absoluteFilePath).matches();
        }
        else {
            return true;
        }
    }

    // validate the path, path must be an absolute path
    public static List<String> buildSourceTargets(List<String> directories)
    {
        Map<String, Boolean> isRegexPath = new HashMap<>();
        Map<String, Pattern> patterns = new HashMap<>();

        // for each path
        Set<String> toBeReadFiles = new HashSet<>();
        for (String eachPath : directories) {
            int endMark;
            for (endMark = 0; endMark < eachPath.length(); endMark++) {
                if ('*' == eachPath.charAt(endMark) || '?' == eachPath.charAt(endMark)) {
                    isRegexPath.put(eachPath, true);
                    patterns.put(eachPath, generatePattern(eachPath));
                    break;
                }
            }

            String parentDirectory;
            if (!isRegexPath.isEmpty() && isRegexPath.get(eachPath)) {
                int lastDirSeparator = eachPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
                parentDirectory = eachPath.substring(0, lastDirSeparator + 1);
            }
            else {
                isRegexPath.put(eachPath, false);
                parentDirectory = eachPath;
            }
            buildSourceTargetsEachPath(eachPath, parentDirectory, toBeReadFiles, patterns, isRegexPath);
        }
        return Arrays.asList(toBeReadFiles.toArray(new String[0]));
    }

    private static void buildSourceTargetsEachPath(String regexPath, String parentDirectory, Set<String> toBeReadFiles,
            Map<String, Pattern> patterns, Map<String, Boolean> isRegexPath)
    {
        // 检测目录是否存在，错误情况更明确
        assert checkDirectoryReadable(parentDirectory);

        directoryRover(regexPath, parentDirectory, toBeReadFiles, patterns, isRegexPath);
    }

    private static void directoryRover(String regexPath, String parentDirectory, Set<String> toBeReadFiles,
            Map<String, Pattern> patterns, Map<String, Boolean> isRegexPath)
    {
        File directory = new File(parentDirectory);
        // is a normal file
        if (!directory.isDirectory()) {
            if (isTargetFile(patterns, isRegexPath, regexPath, directory.getAbsolutePath())) {
                toBeReadFiles.add(parentDirectory);
                LOG.info("add file [{}] as a candidate to be read.", parentDirectory);
            }
        }
        else {
            // 是目录
            try {
                // warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
                File[] files = directory.listFiles();
                if (null != files) {
                    for (File subFileNames : files) {
                        directoryRover(regexPath, subFileNames.getAbsolutePath(), toBeReadFiles, patterns, isRegexPath);
                    }
                }
                else {
                    // warn: 对于没有权限的文件，是直接throw AddaxException
                    String message = String.format("您没有权限查看目录 : [%s]", directory);
                    LOG.error(message);
                    throw new RuntimeException(message);
                }
            }
            catch (SecurityException e) {
                String message = String.format("您没有权限查看目录 : [%s]", directory);
                LOG.error(message);
                throw new RuntimeException(message);
            }
        }
    }

    public static <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber)
    {
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

    public static String generateFileMiddleName()
    {
        String randomChars = "0123456789abcdefghmnpqrstuvwxyz";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
        // like 2021-12-03-14-33-29-237-6587fddb
        return dateFormat.format(new Date()) + "_" + RandomStringUtils.random(8, randomChars);
    }

//    private static String bytesToHexString(byte[] src)
//    {
//        StringBuilder builder = new StringBuilder();
//        if (src == null || src.length <= 0) {
//            return null;
//        }
//        String hv;
//        for (byte b : src) {
//            // 以十六进制（基数 16）无符号整数形式返回一个整数参数的字符串表示形式，并转换为大写
//            hv = Integer.toHexString(b & 0xFF).toUpperCase();
//            if (hv.length() < 2) {
//                builder.append(0);
//            }
//            builder.append(hv);
//        }
//        return builder.toString();
//    }

//    public static String getCompressType(String filePath)
//            throws IOException
//    {
//        FileInputStream fis = new FileInputStream(filePath);
//        return getCompressType(fis);
//    }
//
//    public static String getCompressType(InputStream inputStream)
//            throws IOException
//    {
//        byte[] b = new byte[2];
//        inputStream.read(b, 0, b.length);
//        return mFileTypes.getOrDefault(bytesToHexString(b), null);
//    }
}
