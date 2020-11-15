package com.alibaba.datax.plugin.reader.dbffilereader.utils;

import java.io.DataInput;
import java.io.IOException;

/**
 * Created by zhongtian.hu on 19-8-8.
 */
public final class DbfUtils
{

    private DbfUtils()
    {
    }

    public static int readLittleEndianInt(DataInput in)
            throws IOException
    {
        int bigEndian = 0;
        for (int shiftBy = 0; shiftBy < 32; shiftBy += 8) {
            bigEndian |= (in.readUnsignedByte() & 0xff) << shiftBy;
        }
        return bigEndian;
    }

    public static short readLittleEndianShort(DataInput in)
            throws IOException
    {
        int low = in.readUnsignedByte() & 0xff;
        int high = in.readUnsignedByte();
        return (short) (high << 8 | low);
    }

    public static byte[] trimLeftSpaces(byte[] arr)
    {
        int i = arr.length;
        while (--i >= 0 && arr[i] == ' ');
        byte[] result = new byte[++i];
        if (i > 0) {
            System.arraycopy(arr, 0, result, 0, i);
        }
        return result;
    }

    public static boolean contains(byte[] arr, byte value)
    {
        for (byte anArr : arr) {
            if (anArr == value) {
                return true;
            }
        }

        return false;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @return integer value
     */
    public static int parseInt(byte[] bytes)
    {
        int result = 0;
        for (byte aByte : bytes) {
            if (aByte == ' ') {
                return result;
            }

            result *= 10;
            result += (aByte - (byte) '0');
        }

        return result;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @param from index to start from
     * @param to index to end at
     * @return integer value
     */
    public static int parseInt(byte[] bytes, int from, int to)
    {
        int result = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10;
            result += (bytes[i] - (byte) '0');
        }
        return result;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @return long value
     */
    public static long parseLong(byte[] bytes)
    {
        long result = 0;
        for (byte aByte : bytes) {
            if (aByte == ' ') {
                return result;
            }

            result *= 10;
            result += (aByte - (byte) '0');
        }

        return result;
    }

    /**
     * parses only positive numbers
     *
     * @param bytes bytes of string value
     * @param from index to start from
     * @param to index to end at
     * @return integer value
     */
    public static long parseLong(byte[] bytes, int from, int to)
    {
        long result = 0;
        for (int i = from; i < to && i < bytes.length; i++) {
            result *= 10;
            result += (bytes[i] - (byte) '0');
        }
        return result;
    }
}
