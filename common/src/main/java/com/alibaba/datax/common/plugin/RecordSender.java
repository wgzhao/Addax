package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.element.Record;

public interface RecordSender
{

    Record createRecord();

    void sendToWriter(com.alibaba.datax.common.element.Record record);

    void flush();

    void terminate();

    void shutdown();
}
