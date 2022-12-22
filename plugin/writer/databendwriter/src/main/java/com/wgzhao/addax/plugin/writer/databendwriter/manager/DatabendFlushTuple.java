package com.wgzhao.addax.plugin.writer.databendwriter.manager;

import java.util.List;

public class DatabendFlushTuple
{

    private String label;
    private final Long bytes;
    private final List<byte[]> rows;

    public DatabendFlushTuple(String label, Long bytes, List<byte[]> rows)
    {
        this.label = label;
        this.bytes = bytes;
        this.rows = rows;
    }

    public String getLabel() {return label;}

    public void setLabel(String label) {this.label = label;}

    public Long getBytes() {return bytes;}

    public List<byte[]> getRows() {return rows;}
}
