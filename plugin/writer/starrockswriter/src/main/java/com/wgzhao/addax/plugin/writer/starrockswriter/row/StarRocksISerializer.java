package com.wgzhao.addax.plugin.writer.starrockswriter.row;

import com.wgzhao.addax.common.element.Record;

import java.io.Serializable;

public interface StarRocksISerializer
        extends Serializable
{

    String serialize(Record row);
}
