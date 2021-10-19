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

package com.wgzhao.addax.core.util.container;

import org.apache.commons.lang3.StringUtils;

import java.io.File;

/**
 * Created by jingxing on 14-8-25.
 */
public class CoreConstant
{
    // --------------------------- 全局使用的变量(最好按照逻辑顺序，调整下成员变量顺序)
    // --------------------------------
    public static final String JOB_CONTENT_WRITER_PARAMETER_JOB_ID = "job.content[0].writer.parameter.jobid";
    public static final String JOB_CONTENT_READER_PARAMETER_JOB_ID = "job.content[0].reader.parameter.jobid";
    public static final String CORE_CONTAINER_TASK_GROUP_CHANNEL = "core.container.taskGroup.channel";

    public static final String CORE_CONTAINER_JOB_ID = "core.container.job.id";

    public static final String CORE_CONTAINER_JOB_MODE = "core.container.job.mode";

    public static final String CORE_CONTAINER_JOB_REPORT_INTERVAL = "core.container.job.reportInterval";

    public static final String CORE_CONTAINER_JOB_SLEEP_INTERVAL = "core.container.job.sleepInterval";

    public static final String CORE_CONTAINER_TASK_GROUP_ID = "core.container.taskGroup.id";

    public static final String CORE_CONTAINER_TASK_GROUP_SLEEP_INTERVAL = "core.container.taskGroup.sleepInterval";

    public static final String CORE_CONTAINER_TASK_GROUP_REPORT_INTERVAL = "core.container.taskGroup.reportInterval";

    public static final String CORE_CONTAINER_TASK_FAIL_OVER_MAX_RETRY_TIMES = "core.container.task.failOver.maxRetryTimes";

    public static final String CORE_CONTAINER_TASK_FAIL_OVER_RETRY_INTERVAL_IN_MSEC = "core.container.task.failOver.retryIntervalInMsec";

    public static final String CORE_CONTAINER_TASK_FAIL_OVER_MAX_WAIT_IN_MSEC = "core.container.task.failOver.maxWaitInMsec";

    public static final String CORE_SERVER_ADDRESS = "core.server.address";

    public static final String CORE_SERVER_TIMEOUT_SEC = "core.server.timeout";

    public static final String CORE_TRANSPORT_CHANNEL_CLASS = "core.transport.channel.class";

    public static final String CORE_TRANSPORT_CHANNEL_CAPACITY = "core.transport.channel.capacity";

    public static final String CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE = "core.transport.channel.byteCapacity";

    public static final String CORE_TRANSPORT_CHANNEL_SPEED_BYTE = "core.transport.channel.speed.byte";

    public static final String CORE_TRANSPORT_CHANNEL_SPEED_RECORD = "core.transport.channel.speed.record";

    public static final String CORE_TRANSPORT_CHANNEL_FLOW_CONTROL_INTERVAL = "core.transport.channel.flowControlInterval";

    public static final String CORE_TRANSPORT_EXCHANGER_BUFFER_SIZE = "core.transport.exchanger.bufferSize";

    public static final String CORE_TRANSPORT_RECORD_CLASS = "core.transport.record.class";

    public static final String CORE_STATISTICS_COLLECTOR_PLUGIN_TASK_CLASS = "core.statistics.collector.plugin.taskClass";

    public static final String CORE_STATISTICS_COLLECTOR_PLUGIN_MAX_DIRTY_NUMBER = "core.statistics.collector.plugin.maxDirtyNumber";

    public static final String JOB_CONTENT_READER_NAME = "job.content[0].reader.name";

    public static final String JOB_CONTENT_READER_PARAMETER = "job.content[0].reader.parameter";

    public static final String JOB_CONTENT_WRITER_NAME = "job.content[0].writer.name";

    public static final String JOB_CONTENT_WRITER_PARAMETER = "job.content[0].writer.parameter";

    public static final String JOB_JOB_INFO = "job.jobInfo";

    public static final String JOB_CONTENT = "job.content";

    public static final String JOB_CONTENT_TRANSFORMER = "job.content[0].transformer";

    public static final String JOB_SETTING_SPEED_BYTE = "job.setting.speed.byte";

    public static final String JOB_SETTING_SPEED_RECORD = "job.setting.speed.record";

    public static final String JOB_SETTING_SPEED_CHANNEL = "job.setting.speed.channel";

    public static final String JOB_SETTING_ERROR_LIMIT_RECORD = "job.setting.errorLimit.record";

    public static final String JOB_SETTING_ERROR_LIMIT_PERCENTAGE = "job.setting.errorLimit.percentage";

    public static final String JOB_SETTING_DRY_RUN = "job.setting.dryRun";

    public static final String JOB_PRE_HANDLER_PLUGIN_TYPE = "job.preHandler.pluginType";

    public static final String JOB_PRE_HANDLER_PLUGIN_NAME = "job.preHandler.pluginName";

    public static final String JOB_POST_HANDLER_PLUGIN_TYPE = "job.postHandler.pluginType";

    public static final String JOB_POST_HANDLER_PLUGIN_NAME = "job.postHandler.pluginName";

    public static final String JOB_CONTENT_WRITER_PATH = "job.content[0].writer.parameter.path";
    // ----------------------------- 局部使用的变量
    public static final String JOB_WRITER = "reader";

    public static final String JOB_READER = "reader";

    public static final String JOB_TRANSFORMER = "transformer";

    public static final String JOB_READER_NAME = "reader.name";

    public static final String JOB_READER_PARAMETER = "reader.parameter";

    public static final String JOB_WRITER_NAME = "writer.name";

    public static final String JOB_WRITER_PARAMETER = "writer.parameter";

    public static final String TRANSFORMER_PARAMETER_COLUMN_INDEX = "parameter.columnIndex";
    public static final String TRANSFORMER_PARAMETER_PARAS = "parameter.paras";
    public static final String TRANSFORMER_PARAMETER_CONTEXT = "parameter.context";
    public static final String TRANSFORMER_PARAMETER_CODE = "parameter.code";
    public static final String TRANSFORMER_PARAMETER_EXTRA_PACKAGE = "parameter.extraPackage";

    public static final String TASK_ID = "taskId";


    // ----------------------------- 环境变量 ---------------------------------

    public static final String HOME = System.getProperty("addax.home");

    public static final String CONF_PATH = StringUtils.join(new String[] {
            HOME, "conf", "core.json"}, File.separator);

    public static final String PLUGIN_HOME = StringUtils.join(new String[] {
            HOME, "plugin"}, File.separator);

    public static final String PLUGIN_READER_HOME = StringUtils.join(
            new String[] {PLUGIN_HOME, "reader"}, File.separator);

    public static final String PLUGIN_WRITER_HOME = StringUtils.join(
            new String[] {PLUGIN_HOME, "writer"}, File.separator);

    public static final String STORAGE_TRANSFORMER_HOME = StringUtils.join(
            new String[] {HOME, "local_storage", "transformer"}, File.separator);

}
