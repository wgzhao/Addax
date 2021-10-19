/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.util;

import com.wgzhao.addax.common.spi.ErrorCode;

/**
 * <p>请不要格式化本类代码</p>
 */
public enum FrameworkErrorCode
        implements ErrorCode
{

    ARGUMENT_ERROR("Framework-01", "Addax 引擎运行错误，该问题通常是由于内部编程错误引起，请联系Addax 开发团队解决 ."),
    RUNTIME_ERROR("Framework-02", "Addax 引擎运行过程出错，具体原因请参看Addax 运行结束时的错误诊断信息  ."),
    CONFIG_ERROR("Framework-03", "Addax 引擎配置错误，该问题通常是由于Addax 安装错误引起，请联系您的运维解决 ."),

    PLUGIN_INSTALL_ERROR("Framework-10", "Addax 插件安装错误, 该问题通常是由于Addax 安装错误引起，请联系您的运维解决 ."),
    PLUGIN_INIT_ERROR("Framework-12", "Addax 插件初始化错误, 该问题通常是由于Addax 安装错误引起，请联系您的运维解决 ."),
    PLUGIN_RUNTIME_ERROR("Framework-13", "Addax 插件运行时出错, 具体原因请参看Addax 运行结束时的错误诊断信息 ."),
    PLUGIN_DIRTY_DATA_LIMIT_EXCEED("Framework-14",
            "Addax 传输脏数据超过用户预期，该错误通常是由于源端数据存在较多业务脏数据导致，请仔细检查Addax 汇报的脏数据日志信息, 或者您可以适当调大脏数据阈值 ."),
    PLUGIN_SPLIT_ERROR("Framework-15", "Addax 插件切分出错, 该问题通常是由于Addax 各个插件编程错误引起，请联系Addax 开发团队解决"),
    CALL_REMOTE_FAILED("Framework-19", "远程调用失败"),
    KILLED_EXIT_VALUE("Framework-143", "Job 收到了 Kill 命令.");

    private final String code;

    private final String description;

    FrameworkErrorCode(String code, String description)
    {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode()
    {
        return this.code;
    }

    @Override
    public String getDescription()
    {
        return this.description;
    }

    @Override
    public String toString()
    {
        return String.format("Code:[%s], Description:[%s]. ", this.code, this.description);
    }

    /*
     * 通过 "Framework-143" 来标示 任务是 Killed 状态
     */
    public int toExitValue()
    {
        if (this == FrameworkErrorCode.KILLED_EXIT_VALUE) {
            return 143;
        }
        else {
            return 1;
        }
    }
}
