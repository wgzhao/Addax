package com.wgzhao.addax.core.util;

public class ExceptionTracker
{
    public static final  int STRING_BUFFER = 4096;

    public static String trace(Throwable e) {
        StringBuilder sb = new StringBuilder(STRING_BUFFER);
        sb.append(e.toString()).append("\n");
        StackTraceElement[] stackTrace = e.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            sb.append("\t").append(stackTraceElement).append("\n");
        }
        return sb.toString();
    }
}
