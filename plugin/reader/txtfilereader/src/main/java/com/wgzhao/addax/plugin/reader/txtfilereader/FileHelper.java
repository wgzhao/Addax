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

package com.wgzhao.addax.plugin.reader.txtfilereader;

import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class FileHelper
{
    public static final HashMap<String, String> mFileTypes = new HashMap<>();

    static {
        // images

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

    private static String bytesToHexString(byte[] src)
    {
        StringBuilder builder = new StringBuilder();
        if (src == null || src.length <= 0) {
            return null;
        }
        String hv;
        for (byte b : src) {
            // 以十六进制（基数 16）无符号整数形式返回一个整数参数的字符串表示形式，并转换为大写
            hv = Integer.toHexString(b & 0xFF).toUpperCase();
            if (hv.length() < 2) {
                builder.append(0);
            }
            builder.append(hv);
        }
        return builder.toString();
    }

    public static String getCompressType(String filePath)
            throws IOException
    {
        FileInputStream fis = new FileInputStream(filePath);
        return getCompressType(fis);
    }

    public static String getCompressType(InputStream inputStream)
            throws IOException
    {
        byte[] b = new byte[2];
        inputStream.read(b, 0, b.length);
        return mFileTypes.getOrDefault(bytesToHexString(b), null);
    }

    public static BufferedReader readCompressFile(String fileName, String encoding, int bufferSize)
    {
        BufferedReader reader;
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(fileName);
        }
        catch (FileNotFoundException e) {
            // warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
            throw AddaxException.asAddaxException(
                    TxtFileReaderErrorCode.OPEN_FILE_ERROR, String.format("找不到待读取的文件 : [%s]", fileName));
        }
        try {
            String compressType = FileHelper.getCompressType(fileName);
            if (compressType != null) {
                if ("zip".equals(compressType)) {
                    ZipCycleInputStream zis = new ZipCycleInputStream(inputStream);
                    reader = new BufferedReader(new InputStreamReader(zis, encoding), bufferSize);
                }
                else {
                    BufferedInputStream bis = new BufferedInputStream(inputStream);
                    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
                    reader = new BufferedReader(new InputStreamReader(input, encoding), bufferSize);
                }
            }
            else {
                reader = new BufferedReader(new InputStreamReader(inputStream, encoding), bufferSize);
            }
            return reader;
        }
        catch (CompressorException | IOException e) {
            throw AddaxException.asAddaxException(
                    TxtFileReaderErrorCode.READ_FILE_IO_ERROR,
                    e.getMessage()
            );
        }
    }
}
