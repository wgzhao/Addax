package com.wgzhao.addax.plugin.writer.databendwriter.row;

import com.wgzhao.addax.plugin.writer.databendwriter.DatabendWriterOptions;

public class DatabendSerializerFactory
{

    private DatabendSerializerFactory() {}

    public static DatabendISerializer createSerializer(DatabendWriterOptions writerOptions)
    {
        if ("csv".equalsIgnoreCase(writerOptions.getStreamLoadFormat())) {
            String field_delimiter = writerOptions.getFielddelimiter();
            return new DatabendCsvSerializer("".equals(field_delimiter) ? "," : field_delimiter);
        }
        throw new RuntimeException("Only support format 'csv' to create serialize serializer");
    }
}
