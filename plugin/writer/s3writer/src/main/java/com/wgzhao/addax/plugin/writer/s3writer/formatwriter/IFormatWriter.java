package com.wgzhao.addax.plugin.writer.s3writer.formatwriter;

import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import com.wgzhao.addax.common.util.Configuration;

public interface IFormatWriter {
    void init(Configuration config);
    void write(RecordReceiver lineReceiver, Configuration config,
               TaskPluginCollector taskPluginCollector);
}
