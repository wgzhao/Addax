package com.wgzhao.datax.plugin.storage.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Writer
        extends Closeable
{

    void writeOneRecord(List<String> splitedRows)
            throws IOException;

    void flush()
            throws IOException;

    void close()
            throws IOException;
}
