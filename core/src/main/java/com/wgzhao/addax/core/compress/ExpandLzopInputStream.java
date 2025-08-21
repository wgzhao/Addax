/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Description:
 *
 * This class leverages the open-source LZO decompression code by shevek on GitHub
 * (https://github.com/shevek/lzo-java).
 *
 * The reason for extending LzopInputStream is that the open-source implementation defines
 * LZO_LIBRARY_VERSION as 0x2050, while many LZO files use 0x2060. To decompress those files
 * without throwing an exception, we must accept a higher library version. Since
 * LZO_LIBRARY_VERSION is declared final in the original code and cannot be changed, we
 * extend LzopInputStream and override the header checks to allow 0x2060.
 */

package com.wgzhao.addax.core.compress;

import org.anarres.lzo.LzoVersion;
import org.anarres.lzo.LzopConstants;
import org.anarres.lzo.LzopInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

/**
 * Created by mingya.wmy on 16/8/26.
 */
public class ExpandLzopInputStream
        extends LzopInputStream
{

    public ExpandLzopInputStream(@Nonnull InputStream in)
            throws IOException
    {
        super(in);
    }

    /**
     * Read and verify a lzo header, setting relevant block checksum options
     * and ignoring most everything else.
     */
    @Override
    protected int readHeader()
            throws IOException
    {
        final short lzoLibraryVersion = 0x2060;
        Logger log = LoggerFactory.getLogger(ExpandLzopInputStream.class);
        byte[] lzopMagic = {-119, 'L', 'Z', 'O', 0, '\r', '\n', '\032', '\n'};
        byte[] buf = new byte[9];
        readBytes(buf, 0, 9);
        if (!Arrays.equals(buf, lzopMagic)) {
            throw new IOException("Invalid LZO header");
        }
        Arrays.fill(buf, (byte) 0);
        Adler32 adler = new Adler32();
        CRC32 crc32 = new CRC32();
        int hitem = readHeaderItem(buf, 2, adler, crc32); // lzop version
        if (hitem > LzopConstants.LZOP_VERSION) {
            log.debug("Compressed with later version of lzop: {} (expected 0x{})",
                    Integer.toHexString(hitem), Integer.toHexString(LzopConstants.LZOP_VERSION));
        }
        hitem = readHeaderItem(buf, 2, adler, crc32); // lzo library version
        if (hitem > lzoLibraryVersion) {
            throw new IOException("Compressed with incompatible lzo version: 0x"
                    + Integer.toHexString(hitem) + " (expected 0x"
                    + Integer.toHexString(LzoVersion.LZO_LIBRARY_VERSION) + ")");
        }
        hitem = readHeaderItem(buf, 2, adler, crc32); // lzop extract version
        if (hitem > LzopConstants.LZOP_VERSION) {
            throw new IOException("""
                    Compressed with incompatible lzop version: 0x%s (expected 0x%s)
                    """.formatted(
                    Integer.toHexString(hitem),
                    Integer.toHexString(LzopConstants.LZOP_VERSION)));
        }
        hitem = readHeaderItem(buf, 1, adler, crc32); // method
        switch (hitem) {
            case LzopConstants.M_LZO1X_1, LzopConstants.M_LZO1X_1_15, LzopConstants.M_LZO1X_999 -> {}
            default -> throw new IOException("Invalid strategy: 0x%x".formatted(hitem));
        }
        readHeaderItem(buf, 1, adler, crc32); // ignore level

        // flags
        int flags = readHeaderItem(buf, 4, adler, crc32);
        boolean useCRC32 = (flags & LzopConstants.F_H_CRC32) != 0;
        boolean extraField = (flags & LzopConstants.F_H_EXTRA_FIELD) != 0;
        if ((flags & LzopConstants.F_MULTIPART) != 0) {
            throw new IOException("Multipart lzop not supported");
        }
        if ((flags & LzopConstants.F_H_FILTER) != 0) {
            throw new IOException("lzop filter not supported");
        }
        if ((flags & LzopConstants.F_RESERVED) != 0) {
            throw new IOException("Unknown flags in header");
        }
        // known !F_H_FILTER, so no optional block

        readHeaderItem(buf, 4, adler, crc32); // ignore mode
        readHeaderItem(buf, 4, adler, crc32); // ignore mtime
        readHeaderItem(buf, 4, adler, crc32); // ignore gmtdiff
        hitem = readHeaderItem(buf, 1, adler, crc32); // fn len
        if (hitem > 0) {
            byte[] tmp = (hitem > buf.length) ? new byte[hitem] : buf;
            readHeaderItem(tmp, hitem, adler, crc32); // skip filename
        }
        int checksum = (int) (useCRC32 ? crc32.getValue() : adler.getValue());
        hitem = readHeaderItem(buf, 4, adler, crc32); // read checksum
        if (hitem != checksum) {
            throw new IOException("""
                    Invalid header checksum: %s (expected 0x%s)
                    """.formatted(
                    Long.toHexString(checksum),
                    Integer.toHexString(hitem)));
        }
        if (extraField) { // lzop 1.08 ultimately ignores this
            log.debug("Extra header field not processed");
            adler.reset();
            crc32.reset();
            hitem = readHeaderItem(buf, 4, adler, crc32);
            readHeaderItem(new byte[hitem], hitem, adler, crc32);
            checksum = (int) (useCRC32 ? crc32.getValue() : adler.getValue());
            if (checksum != readHeaderItem(buf, 4, adler, crc32)) {
                throw new IOException("Invalid checksum for extra header field");
            }
        }

        return flags;
    }

    private int readHeaderItem(@Nonnull byte[] buf, @Nonnegative int len, @Nonnull Adler32 adler, @Nonnull CRC32 crc32)
            throws IOException
    {
        Objects.requireNonNull(buf, "Buffer cannot be null");
        Objects.requireNonNull(adler, "Adler32 cannot be null");
        Objects.requireNonNull(crc32, "CRC32 cannot be null");

        int ret = readInt(buf, len);
        adler.update(buf, 0, len);
        crc32.update(buf, 0, len);
        Arrays.fill(buf, (byte) 0);
        return ret;
    }

    /**
     * Read len bytes into buf, st LSB of int returned is the last byte of the
     * first word read.
     *
     * @param buf bytes including integer
     * @param len the length of bytes buffer
     * @return integer
     * @throws IOException read exception
     */
    private int readInt(@Nonnull byte[] buf, @Nonnegative int len)
            throws IOException
    {
        readBytes(buf, 0, len);
        int ret = (0xFF & buf[0]) << 24;
        ret |= (0xFF & buf[1]) << 16;
        ret |= (0xFF & buf[2]) << 8;
        ret |= (0xFF & buf[3]);
        return (len > 3) ? ret : (ret >>> (8 * (4 - len)));
    }
}
