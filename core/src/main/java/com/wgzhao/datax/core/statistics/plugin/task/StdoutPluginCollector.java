package com.wgzhao.datax.core.statistics.plugin.task;

import com.wgzhao.datax.common.constant.PluginType;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.statistics.communication.Communication;
import com.wgzhao.datax.core.statistics.plugin.task.util.DirtyRecord;
import com.wgzhao.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jingxing on 14-9-9.
 */
public class StdoutPluginCollector
        extends AbstractTaskPluginCollector
{
    private static final Logger LOG = LoggerFactory
            .getLogger(StdoutPluginCollector.class);

    private static final int DEFAULT_MAX_DIRTYNUM = 128;
    private final AtomicInteger currentLogNum = new AtomicInteger(0);
    private AtomicInteger maxLogNum = new AtomicInteger(0);

    public StdoutPluginCollector(Configuration configuration, Communication communication,
            PluginType type)
    {
        super(configuration, communication, type);
        maxLogNum = new AtomicInteger(
                configuration.getInt(
                        CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_MAXDIRTYNUM,
                        DEFAULT_MAX_DIRTYNUM));
    }

    private String formatDirty(final Record dirty, final Throwable t,
            final String msg)
    {
        Map<String, Object> msgGroup = new HashMap<>();

        msgGroup.put("type", super.getPluginType().toString());
        if (StringUtils.isNotBlank(msg)) {
            msgGroup.put("message", msg);
        }
        if (null != t && StringUtils.isNotBlank(t.getMessage())) {
            msgGroup.put("exception", t.getMessage());
        }
        if (null != dirty) {
            msgGroup.put("record", DirtyRecord.asDirtyRecord(dirty)
                    .getColumns());
        }

        return JSON.toJSONString(msgGroup);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
            String errorMessage)
    {
        int logNum = currentLogNum.getAndIncrement();
        if (logNum == 0 && t != null) {
            LOG.error("", t);
        }
        if (maxLogNum.intValue() < 0 || currentLogNum.intValue() < maxLogNum.intValue()) {
            LOG.error("脏数据: {}%n", this.formatDirty(dirtyRecord, t, errorMessage));
        }

        super.collectDirtyRecord(dirtyRecord, t, errorMessage);
    }
}
