package com.alibaba.datax.plugin.reader.dbffilereader.utils;

/**
 * Created by zhongtian.hu on 19-8-8.
 */
public class StringUtils
{

    public static String rightPad(String str, int size, char padChar)
    {
        // returns original string when possible
        if (str.length() >= size) {
            return str;
        }

        StringBuilder sb = new StringBuilder(size + 1).append(str);
        while (sb.length() < size) {
            sb.append(padChar);
        }
        return sb.toString();
    }
}