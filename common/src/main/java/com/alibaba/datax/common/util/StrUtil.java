package com.alibaba.datax.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StrUtil
{

    private static final long KB_IN_BYTES = 1024L;

    private static final long MB_IN_BYTES = 1024 * KB_IN_BYTES;

    private static final long GB_IN_BYTES = 1024 * MB_IN_BYTES;

    private static final long TB_IN_BYTES = 1024 * GB_IN_BYTES;

    private static final DecimalFormat df = new DecimalFormat("0.00");

    private static final Pattern VARIABLE_PATTERN = Pattern
            .compile("(\\$)\\{?(\\w+)\\}?");

    private static String systemEncoding = System.getProperty("file.encoding");

    private StrUtil()
    {
    }

    public static String stringify(long byteNumber)
    {
        if (byteNumber / TB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) TB_IN_BYTES) + "TB";
        }
        else if (byteNumber / GB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) GB_IN_BYTES) + "GB";
        }
        else if (byteNumber / MB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) MB_IN_BYTES) + "MB";
        }
        else if (byteNumber / KB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) KB_IN_BYTES) + "KB";
        }
        else {
            return byteNumber + "B";
        }
    }

    public static String replaceVariable(final String param)
    {
        Map<String, String> mapping = new HashMap<>();

        Matcher matcher = VARIABLE_PATTERN.matcher(param);
        while (matcher.find()) {
            String variable = matcher.group(2);
            String value = System.getProperty(variable);
            if (StringUtils.isBlank(value)) {
                value = matcher.group();
            }
            mapping.put(matcher.group(), value);
        }

        String retString = param;
        for (Map.Entry<String, String> entry: mapping.entrySet()) {
            retString = retString.replace(entry.getKey(), entry.getValue());
        }

        return retString;
    }

    public static String compressMiddle(String s, int headLength, int tailLength)
    {
        Validate.notNull(s, "Input string must not be null");
        Validate.isTrue(headLength > 0, "Head length must be larger than 0");
        Validate.isTrue(tailLength > 0, "Tail length must be larger than 0");

        if (headLength + tailLength >= s.length()) {
            return s;
        }
        return s.substring(0, headLength) + "..." + s.substring(s.length() - tailLength);
    }

    static {
        if (systemEncoding == null) {
            systemEncoding = "UTF-8";
        }
    }
}
