package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class OrcWriter {
    private Writer writer;
    private TypeDescription schema;

    private org.apache.hadoop.conf.Configuration hadoopConf;
    private Path path;

    private final Configuration writerSliceConfig;

    public OrcWriter(Configuration writerSliceConfig) {
        this.writerSliceConfig = writerSliceConfig;
    }

    public  TypeDescription createSchema() {
        List<Configuration> columns = writerSliceConfig.getListConfiguration(Key.COLUMN);
        String compress = writerSliceConfig.getString(Key.COMPRESS, "NONE").toUpperCase();
        StringJoiner joiner = new StringJoiner(",");
        for (Configuration column : columns) {
            if ("decimal".equals(column.getString(Key.TYPE))) {
                joiner.add(String.format("%s:%s(%s,%s)", column.getString(Key.NAME), "decimal",
                        column.getInt(Key.PRECISION, Constant.DEFAULT_DECIMAL_MAX_PRECISION),
                        column.getInt(Key.SCALE, Constant.DEFAULT_DECIMAL_MAX_SCALE)));
            } else {
                joiner.add(String.format("%s:%s", column.getString(Key.NAME), column.getString(Key.TYPE)));
            }
        }
        return TypeDescription.fromString("struct<" + joiner + ">");
    }

    public void setRow() {

    }
}
