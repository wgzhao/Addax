/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.reader.hdfsreader;

import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.orc.OrcFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;

import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

/** File Type Utils. */
public class FileTypeUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(FileTypeUtils.class);

    /** The four bytes every sequence file and every parquet file starts with. */
    private static final byte[] PARQUET_MAGIC = {'P', 'A', 'R', '1'};
    private static final byte[] SEQUENCE_MAGIC = {'S', 'E', 'Q'};
    /** The magic of an ORC file, written as the first bytes and as the tail of the postscript. */
    private static final byte[] ORC_MAGIC = OrcFile.MAGIC.getBytes(StandardCharsets.US_ASCII);

    private FileTypeUtils() {}

    private static boolean isSequenceFile(FileStatus status, FSDataInputStream in)
    {
        byte[] magic = new byte[SEQUENCE_MAGIC.length];
        try {
            in.seek(0);
            in.readFully(magic);
            return Arrays.equals(magic, SEQUENCE_MAGIC);
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not Sequence file.", status.getPath());
        }
        return false;
    }

    /**
     * A parquet file starts with the {@code PAR1} magic and ends with the length of its footer
     * followed by the same magic.
     * <p>
     * The check used to build a reader and read its first record, which opened the file a second
     * time with a default configuration - ignoring the {@code hadoopConfig} and the Kerberos
     * settings of the job - cost a record decode per file, and called a valid parquet file that
     * holds no record a file of another format.
     *
     * @param status the file to look at
     * @param in the open stream of the file
     * @return true when the file is a parquet file
     */
    private static boolean isParquetFile(FileStatus status, FSDataInputStream in)
    {
        try {
            byte[] magic = new byte[PARQUET_MAGIC.length];
            in.seek(0);
            in.readFully(magic);
            // the file has to hold the magic at both ends, which also rejects a short file
            if (!Arrays.equals(magic, PARQUET_MAGIC) || status.getLen() < 2L * magic.length) {
                return false;
            }
            in.seek(status.getLen() - magic.length);
            in.readFully(magic);
            return Arrays.equals(magic, PARQUET_MAGIC);
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not parquet file.", status.getPath());
        }
        return false;
    }

    /**
     * Whether the bytes at the given offset of the file are the given magic.
     *
     * @param bytes the bytes of the file
     * @param offset the offset to look at
     * @param magic the magic to compare with
     * @return true when the bytes match
     */
    private static boolean magicAt(byte[] bytes, int offset, byte[] magic)
    {
        return offset >= 0 && offset + magic.length <= bytes.length
                && Arrays.equals(bytes, offset, offset + magic.length, magic, 0, magic.length);
    }

    private static boolean isORCFile(FileStatus status, FSDataInputStream in)
    {
        final int DIRECTORY_SIZE_GUESS = 16 * 1024;
        try {
            // figure out the size of the file using the option or filesystem
            long size = status.getLen();

            //read last bytes into buffer to get PostScript
            int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
            byte[] buffer = new byte[readSize];
            in.seek(size - readSize);
            in.readFully(buffer);

            //read the PostScript
            //get length of PostScript
            int psLen = buffer[readSize - 1] & 0xff;
            if (psLen < ORC_MAGIC.length + 1) {
                return false;
            }
            // now look for the magic string at the end of the postscript.
            if (magicAt(buffer, readSize - 1 - ORC_MAGIC.length, ORC_MAGIC)) {
                return true;
            }
            // If it isn't there, this may be the 0.11.0 version of ORC.
            // Read the first 3 bytes of the file to check for the header
            in.seek(0);
            byte[] header = new byte[ORC_MAGIC.length];
            in.readFully(header);
            // if it isn't there, this isn't an ORC file
            return Arrays.equals(header, ORC_MAGIC);
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not ORC file.", status.getPath());
        }
        return false;
    }

    /**
     * Whether a file holds the file type the job asked for.
     *
     * @param hadoopConf the configuration of the job
     * @param status the file to check
     * @param specifiedFileType the configured file type
     * @return true when the file matches the configured type
     */
    public static boolean checkHdfsFileType(org.apache.hadoop.conf.Configuration hadoopConf, FileStatus status, String specifiedFileType)
    {
        String fileType = specifiedFileType.toUpperCase(Locale.ROOT);
        // a text file carries nothing that names its format, every file is a candidate
        if (HdfsConstant.CSV.equals(fileType) || HdfsConstant.TEXT.equals(fileType)) {
            return true;
        }

        try (var in = FileSystem.get(hadoopConf).open(status.getPath())) {
            // the file system is cached per JVM and shared with every task of the job, closing it
            // here closed it for the other holders as well
            return switch (fileType) {
                case HdfsConstant.ORC -> isORCFile(status, in);
                case HdfsConstant.SEQ -> isSequenceFile(status, in);
                case HdfsConstant.PARQUET -> isParquetFile(status, in);
                default -> false;
            };
        }
        catch (Exception e) {
            var message = """
                    Can not get the file format for [%s], it only supports [%s].
                    """.formatted(status.getPath(), HdfsConstant.SUPPORT_FILE_TYPE);
            LOG.error(message, e);
            throw AddaxException.asAddaxException(EXECUTE_FAIL, message, e);
        }
    }
}
