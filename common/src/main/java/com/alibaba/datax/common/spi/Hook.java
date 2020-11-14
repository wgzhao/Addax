package com.alibaba.datax.common.spi;

import com.alibaba.datax.common.util.Configuration;

import java.util.Map;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 */
public interface Hook
{

    String getName();

    void invoke(Configuration jobConf, Map<String, Number> msg);
}
