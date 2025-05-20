package com.wgzhao.addax.plugin.writer.s3writer.writer;

import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import com.wgzhao.addax.core.util.Configuration;

public interface IFormatWriter
{
    void init(Configuration config);

    void write(RecordReceiver lineReceiver, Configuration config,
            TaskPluginCollector taskPluginCollector);
}
