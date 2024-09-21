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

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.communication.CommunicationTool;
import com.wgzhao.addax.core.util.container.CoreConstant;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wgzhao.addax.common.exception.ErrorCode.OVER_LIMIT_ERROR;

/**
 * 检查任务是否到达错误记录限制。有检查条数（recordLimit）和百分比(percentageLimit)两种方式。
 * 1. errorRecord表示出错条数不能大于限制数，当超过时任务失败。比如errorRecord为0表示不容许任何脏数据。
 * 2. errorPercentage表示出错比例，在任务结束时校验。
 * 3. errorRecord优先级高于errorPercentage。
 */
public final class ErrorRecordChecker
{
    private static final Logger LOG = LoggerFactory.getLogger(ErrorRecordChecker.class);

    private final Long recordLimit;
    private Double percentageLimit;

    public ErrorRecordChecker(Configuration configuration)
    {
        this(configuration.getLong(CoreConstant.JOB_SETTING_ERROR_LIMIT_RECORD),
                configuration.getDouble(CoreConstant.JOB_SETTING_ERROR_LIMIT_PERCENTAGE));
    }

    public ErrorRecordChecker(Long rec, Double percentage)
    {
        recordLimit = rec;
        percentageLimit = percentage;

        if (percentageLimit != null) {
            Validate.isTrue(0.0 <= percentageLimit && percentageLimit <= 1.0,
                    "The dirty data percentage limit should be between 0.0 and 1.0");
        }

        if (recordLimit != null) {
            Validate.isTrue(recordLimit >= 0, "The number of dirty data records now greater than zero");

            // errorRecord优先级高于errorPercentage.
            percentageLimit = null;
        }
    }

    public void checkRecordLimit(Communication communication)
    {
        if (recordLimit == null) {
            return;
        }

        long errorNumber = CommunicationTool.getTotalErrorRecords(communication);
        if (recordLimit < errorNumber) {
            LOG.debug("The error limit is set to {}%. The error counter was checked.", recordLimit);
            throw AddaxException.asAddaxException(
                    OVER_LIMIT_ERROR,
                    String.format("The number of dirty data records did not pass the check. " +
                                    "The limit is [%d] records, but [%d] records were actually captured.",
                            recordLimit, errorNumber));
        }
    }

    public void checkPercentageLimit(Communication communication)
    {
        if (percentageLimit == null) {
            return;
        }
        LOG.debug("The error limit is set to {}%. The error percentage was checked.", percentageLimit);

        long total = CommunicationTool.getTotalReadRecords(communication);
        long error = CommunicationTool.getTotalErrorRecords(communication);

        if (total > 0 && ((double) error / (double) total) > percentageLimit) {
            throw AddaxException.asAddaxException(
                    OVER_LIMIT_ERROR,
                    String.format("The dirty data percentage check failed. The limit is [%f], but [%f] was actually captured",
                            percentageLimit, ((double) error / (double) total)));
        }
    }
}
