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

import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.Text;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;

public class FileTypeUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(FileTypeUtils.class);

    private static boolean isSequenceFile(Path filepath, FSDataInputStream in)
    {
        final byte[] seqMagic = {(byte) 'S', (byte) 'E', (byte) 'Q'};
        byte[] magic = new byte[seqMagic.length];
        try {
            in.seek(0);
            in.readFully(magic);
            return Arrays.equals(magic, seqMagic);
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not Sequence file.", filepath);
        }
        return false;
    }

    private static boolean isParquetFile(Path file)
    {
        try {
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, file);
            ParquetReader<Group> build = reader.build();
            if (build.read() != null) {
                return true;
            }
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not parquet file.", file);
        }
        return false;
    }

    private static boolean isRCFile(org.apache.hadoop.conf.Configuration hadoopConf, String filepath, FSDataInputStream in)
    {

        // The first version of RCFile used the sequence file header.
        final byte[] originalMagic = {(byte) 'S', (byte) 'E', (byte) 'Q'};
        // The 'magic' bytes at the beginning of the RCFile
        final byte[] rcMagic = {(byte) 'R', (byte) 'C', (byte) 'F'};
        // the version that was included with the original magic, which is mapped
        // into ORIGINAL_VERSION
        final byte ORIGINAL_MAGIC_VERSION_WITH_METADATA = 6;
        // All the versions should be place in this list.
        final int ORIGINAL_VERSION = 0;  // version with SEQ
        // version with RCF
        // final int NEW_MAGIC_VERSION = 1
        // final int CURRENT_VERSION = NEW_MAGIC_VERSION
        final int CURRENT_VERSION = 1;
        byte version;

        byte[] magic = new byte[rcMagic.length];
        try {
            in.seek(0);
            in.readFully(magic);

            if (Arrays.equals(magic, originalMagic)) {
                if (in.readByte() != ORIGINAL_MAGIC_VERSION_WITH_METADATA) {
                    return false;
                }
                version = ORIGINAL_VERSION;
            }
            else {
                if (!Arrays.equals(magic, rcMagic)) {
                    return false;
                }

                // Set 'version'
                version = in.readByte();
                if (version > CURRENT_VERSION) {
                    return false;
                }
            }

            if (version == ORIGINAL_VERSION) {
                try {
                    Class<?> keyCls = hadoopConf.getClassByName(Text.readString(in));
                    Class<?> valCls = hadoopConf.getClassByName(Text.readString(in));
                    if (!keyCls.equals(RCFile.KeyBuffer.class) || !valCls.equals(RCFile.ValueBuffer.class)) {
                        return false;
                    }
                }
                catch (ClassNotFoundException e) {
                    return false;
                }
            }
//            boolean decompress = in.readBoolean(); // is compressed?
            if (version == ORIGINAL_VERSION) {
                // is block-compressed? it should be always false.
                boolean blkCompressed = in.readBoolean();
                return !blkCompressed;
            }
            return true;
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not RC file.", filepath);
        }
        return false;
    }

    private static boolean isORCFile(Path file, FileSystem fs, FSDataInputStream in)
    {
        final int DIRECTORY_SIZE_GUESS = 16 * 1024;
        try {
            // figure out the size of the file using the option or filesystem
            long size = fs.getFileStatus(file).getLen();

            //read last bytes into buffer to get PostScript
            int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
            in.seek(size - readSize);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.get(readSize - 1) & 0xff;
            String orcMagic = org.apache.orc.OrcFile.MAGIC;
            int len = orcMagic.length();
            if (psLen < len + 1) {
                return false;
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1
                    - len;
            byte[] array = buffer.array();
            // now look for the magic string at the end of the postscript.
            if (Text.decode(array, offset, len).equals(orcMagic)) {
                return true;
            }
            else {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                in.seek(0);
                byte[] header = new byte[len];
                in.readFully(header, 0, len);
                // if it isn't there, this isn't an ORC file
                if (Text.decode(header, 0, len).equals(orcMagic)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            LOG.info("The file [{}] is not ORC file.", file);
        }
        return false;
    }

    public static boolean checkHdfsFileType(org.apache.hadoop.conf.Configuration hadoopConf, String filepath, String specifiedFileType)
    {

        Path file = new Path(filepath);

        try (FileSystem fs = FileSystem.get(hadoopConf); FSDataInputStream in = fs.open(file)) {
            if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.ORC)) {
                return isORCFile(file, fs, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.RC)) {
                return isRCFile(hadoopConf, filepath, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.SEQ)) {

                return isSequenceFile(file, in);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.PARQUET)) {
                return isParquetFile(file);
            }
            else if (StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.CSV)
                    || StringUtils.equalsIgnoreCase(specifiedFileType, HdfsConstant.TEXT)) {
                return true;
            }
        }
        catch (Exception e) {
            String message = String.format("Can not get the file format for [%s]，it only supports [%s].",
                    filepath, HdfsConstant.SUPPORT_FILE_TYPE);
            LOG.error(message);
            throw AddaxException.asAddaxException(EXECUTE_FAIL, message, e);
        }
        return false;
    }
}
