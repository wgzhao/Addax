package com.wgzhao.datax.common.plugin;

import com.wgzhao.datax.common.element.Record;

public interface RecordSender
{

    Record createRecord();

    void sendToWriter(Record record);

    void flush();

    void terminate();

    void shutdown();
}
