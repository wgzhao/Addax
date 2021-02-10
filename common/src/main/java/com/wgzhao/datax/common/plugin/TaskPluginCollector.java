package com.wgzhao.datax.common.plugin;

import com.wgzhao.datax.common.element.Record;

/**
 * 该接口提供给Task Plugin用来记录脏数据和自定义信息。 <br >
 * <p>
 * 1. 脏数据记录，TaskPluginCollector提供多种脏数据记录的适配，包括本地输出、集中式汇报等等<br >
 * 2. 自定义信息，所有的task插件运行过程中可以通过TaskPluginCollector收集信息， <br >
 * Job的插件在POST过程中通过getMessage()接口获取信息
 */
public abstract class TaskPluginCollector
        implements PluginCollector
{
    /**
     * 收集脏数据
     *
     * @param dirtyRecord 脏数据信息
     * @param t 异常信息
     * @param errorMessage 错误的提示信息
     */
    public abstract void collectDirtyRecord( Record dirtyRecord,
             Throwable t,  String errorMessage);

    /**
     * 收集脏数据
     *
     * @param dirtyRecord 脏数据信息
     * @param errorMessage 错误的提示信息
     */
    public void collectDirtyRecord( Record dirtyRecord,
             String errorMessage)
    {
        this.collectDirtyRecord(dirtyRecord, null, errorMessage);
    }

    /**
     * 收集脏数据
     *
     * @param dirtyRecord 脏数据信息
     * @param t 异常信息
     */
    public void collectDirtyRecord( Record dirtyRecord,  Throwable t)
    {
        this.collectDirtyRecord(dirtyRecord, t, "");
    }

    /**
     * 收集自定义信息，Job插件可以通过getMessage获取该信息 <br >
     * 如果多个key冲突，内部使用List记录同一个key，多个value情况。<br >
     * @param key message key
     * @param value message content
     */
    public abstract void collectMessage( String key,  String value);
}
