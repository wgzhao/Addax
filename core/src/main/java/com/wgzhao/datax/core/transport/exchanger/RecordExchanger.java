package com.wgzhao.datax.core.transport.exchanger;

import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.exception.CommonErrorCode;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.common.plugin.RecordReceiver;
import com.wgzhao.datax.common.plugin.RecordSender;
import com.wgzhao.datax.common.plugin.TaskPluginCollector;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.transport.channel.Channel;
import com.wgzhao.datax.core.transport.record.TerminateRecord;
import com.wgzhao.datax.core.transport.transformer.TransformerExecution;
import com.wgzhao.datax.core.util.FrameworkErrorCode;
import com.wgzhao.datax.core.util.container.CoreConstant;

import java.util.List;

public class RecordExchanger
        extends TransformerExchanger
        implements RecordSender, RecordReceiver
{

    private static Class<? extends Record> RECORD_CLASS;
    private final Channel channel;
    private volatile boolean shutdown = false;

    @SuppressWarnings("unchecked")
    public RecordExchanger(int taskGroupId, int taskId, Channel channel, Communication communication,
            List<TransformerExecution> transformerExecs, TaskPluginCollector pluginCollector)
    {
        super(taskGroupId, taskId, communication, transformerExecs, pluginCollector);
        assert channel != null;
        this.channel = channel;
        Configuration configuration = channel.getConfiguration();
        try {
            RecordExchanger.RECORD_CLASS = (Class<? extends Record>) Class
                    .forName(configuration.getString(
                            CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS,
                            "com.wgzhao.datax.core.transport.record.DefaultRecord"));
        }
        catch (ClassNotFoundException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public Record getFromReader()
    {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        Record record = this.channel.pull();
        return (record instanceof TerminateRecord ? null : record);
    }

    @Override
    public Record createRecord()
    {
        try {
            return RECORD_CLASS.newInstance();
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record)
    {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        record = doTransformer(record);
        if (record == null) {
            return;
        }
        this.channel.push(record);
        //和channel的统计保持同步
        doStat();
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void terminate()
    {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        this.channel.pushTerminate(TerminateRecord.get());
        //和channel的统计保持同步
        doStat();
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
    }
}
