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
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FileHelper
{
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

    /**
     * detect the specified file compression type
     *
     * @param fileName the file name
     * @return the compression type if present, otherwise ""
     */
    public static String getFileCompressType(String fileName)
    {
        try {
            InputStream inputStream = new FileInputStream(fileName);
            return CompressorStreamFactory.detect(inputStream);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + fileName, e);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException("does not support mark", e);
        }
        catch (CompressorException e) {
            return "";
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
}
