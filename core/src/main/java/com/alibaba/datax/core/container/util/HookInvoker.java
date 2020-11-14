package com.alibaba.datax.core.container.util;

import com.alibaba.datax.common.util.Configuration;

import java.io.File;
import java.util.Map;

/**
 * 扫描给定目录的所有一级子目录，每个子目录当作一个Hook的目录。
 * 对于每个子目录，必须符合ServiceLoader的标准目录格式，见http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html。
 * 加载里头的jar，使用ServiceLoader机制调用。
 */
public class HookInvoker
{

    public HookInvoker(String baseDirName, Configuration conf, Map<String, Number> msg)
    {
        File baseDir = new File(baseDirName);
    }
}
