package com.wgzhao.addax.plugin.writer.databendwriter.row;

import com.wgzhao.addax.common.element.Record;

import java.io.Serializable;

public interface DatabendISerializer
        extends Serializable
{

    String serialize(Record row);
}
