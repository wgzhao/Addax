package com.alibaba.datax.plugin.reader.dbffilereader.exception;


/**
 * @author Sergey Polovko
 */
public class DbfException extends RuntimeException {

    public DbfException(String message, Throwable cause) {
        super(message, cause);
    }

    public DbfException(String message) {
        super(message);
    }
}