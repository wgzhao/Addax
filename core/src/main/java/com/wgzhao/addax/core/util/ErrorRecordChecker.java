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

import static com.wgzhao.addax.common.spi.ErrorCode.OVER_LIMIT_ERROR;

/**
 * Check whether the task has reached the error record limit. There are two ways to check: by record count (recordLimit) and
 *  by percentage (percentageLimit).
 * 1. errorRecord indicates that the number of error records cannot exceed the limit. If it does, the task fails.
 *  For example, an errorRecord of 0 means that no dirty data is allowed.
 * 2. errorPercentage indicates the error ratio, which is checked when the task ends.
 * 3. errorRecord takes precedence over errorPercentage.
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
