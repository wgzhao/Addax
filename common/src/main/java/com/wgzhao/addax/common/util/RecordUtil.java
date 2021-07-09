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

package com.wgzhao.addax.common.util;

import com.wgzhao.addax.common.constant.Type;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.ColumnEntry;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.exception.ColumnErrorCode;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.plugin.TaskPluginCollector;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;

public class RecordUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(RecordUtil.class);

    public static Record transportOneRecord(RecordSender recordSender, List<ColumnEntry> columnConfigs, String[] sourceLine,
            String nullFormat, TaskPluginCollector taskPluginCollector)
    {
        Record record = recordSender.createRecord();
        Column columnGenerated;

        // 创建都为String类型column的record
        if (null == columnConfigs || columnConfigs.isEmpty()) {
            for (String columnValue : sourceLine) {
                // not equalsIgnoreCase, it's all ok if nullFormat is null
                if (columnValue.equals(nullFormat)) {
                    columnGenerated = new StringColumn(null);
                }
                else {
                    columnGenerated = new StringColumn(columnValue);
                }
                record.addColumn(columnGenerated);
            }
            recordSender.sendToWriter(record);
        }
        else {
            try {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue;

                    if (null == columnIndex && null == columnConst) {
                        throw AddaxException
                                .asAddaxException(
                                        ColumnErrorCode.NO_INDEX_VALUE,
                                        "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnConst) {
                        throw AddaxException
                                .asAddaxException(
                                        ColumnErrorCode.MIXED_INDEX_VALUE,
                                        "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }

                    if (null != columnIndex) {
                        if (columnIndex >= sourceLine.length) {
                            String message = String
                                    .format("您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列, 数据详情[%s]",
                                            sourceLine.length, columnIndex + 1,
                                            StringUtils.join(sourceLine, ","));
                            LOG.warn(message);
                            throw new IndexOutOfBoundsException(message);
                        }

                        columnValue = sourceLine[columnIndex];
                    }
                    else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    // it's all ok if nullFormat is null
                    if (columnValue!= null && columnValue.equals(nullFormat)) {
                        columnValue = null;
                    }
                    String errorTemplate = "类型转换错误, 无法将[%s] 转换为[%s]";
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(columnValue);
                            break;
                        case LONG:
                            try {
                                columnGenerated = new LongColumn(columnValue);
                            }
                            catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        errorTemplate, columnValue,
                                        "LONG"));
                            }
                            break;
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue);
                            }
                            catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        errorTemplate, columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue);
                            }
                            catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        errorTemplate, columnValue,
                                        "BOOLEAN"));
                            }

                            break;
                        case DATE:
                            try {
                                if (columnValue == null) {
                                    Date date = null;
                                    columnGenerated = new DateColumn(date);
                                }
                                else {
                                    String formatString = columnConfig.getFormat();
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换, 脏数据行为出现变化
                                        DateFormat format = columnConfig
                                                .getDateFormat();
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    }
                                    else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                            }
                            catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        errorTemplate, columnValue,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "您配置的列类型暂不支持 : [%s]", columnType);
                            LOG.error(errorMessage);
                            throw AddaxException
                                    .asAddaxException(
                                            ColumnErrorCode.NOT_SUPPORT_TYPE,
                                            errorMessage);
                    }

                    record.addColumn(columnGenerated);
                }
                recordSender.sendToWriter(record);
            }
            catch (IllegalArgumentException | IndexOutOfBoundsException iae) {
                taskPluginCollector.collectDirtyRecord(record, iae.getMessage());
            }
            catch (Exception e) {
                if (e instanceof AddaxException) {
                    throw (AddaxException) e;
                }
                // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
                taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }

        return record;
    }
}
