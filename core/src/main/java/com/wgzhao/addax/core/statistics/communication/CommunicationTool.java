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

package com.wgzhao.addax.core.statistics.communication;

import com.wgzhao.addax.common.statistics.PerfTrace;
import com.wgzhao.addax.common.util.StrUtil;
import org.apache.commons.lang3.Validate;

import java.text.DecimalFormat;

/**
 * 这里主要是业务层面的处理
 */
public final class CommunicationTool
{
    public static final String STAGE = "stage";
    public static final String BYTE_SPEED = "byteSpeed";
    public static final String RECORD_SPEED = "recordSpeed";
    public static final String PERCENTAGE = "percentage";

    public static final String READ_SUCCEED_RECORDS = "readSucceedRecords";
    public static final String READ_SUCCEED_BYTES = "readSucceedBytes";

    public static final String READ_FAILED_RECORDS = "readFailedRecords";
    public static final String READ_FAILED_BYTES = "readFailedBytes";

    public static final String WRITE_RECEIVED_RECORDS = "writeReceivedRecords";
    public static final String WRITE_RECEIVED_BYTES = "writeReceivedBytes";

    public static final String WRITE_FAILED_RECORDS = "writeFailedRecords";
    public static final String WRITE_FAILED_BYTES = "writeFailedBytes";

    public static final String TOTAL_READ_RECORDS = "totalReadRecords";
    public static final String WAIT_WRITER_TIME = "waitWriterTime";
    public static final String WAIT_READER_TIME = "waitReaderTime";
    public static final String TRANSFORMER_USED_TIME = "totalTransformerUsedTime";
    public static final String TRANSFORMER_SUCCEED_RECORDS = "totalTransformerSuccessRecords";
    public static final String TRANSFORMER_FAILED_RECORDS = "totalTransformerFailedRecords";
    public static final String TRANSFORMER_FILTER_RECORDS = "totalTransformerFilterRecords";
    private static final String TOTAL_READ_BYTES = "totalReadBytes";
    private static final String TOTAL_ERROR_RECORDS = "totalErrorRecords";
    private static final String TOTAL_ERROR_BYTES = "totalErrorBytes";
    private static final String WRITE_SUCCEED_RECORDS = "writeSucceedRecords";
    private static final String WRITE_SUCCEED_BYTES = "writeSucceedBytes";
    //public static final String TRANSFORMER_NAME_PREFIX = "usedTimeByTransformer_"

    private CommunicationTool() {}

    public static Communication getReportCommunication(Communication now, Communication old, int totalStage)
    {
        Validate.isTrue(now != null && old != null,
                "为汇报准备的新旧metric不能为null");

        long totalReadRecords = getTotalReadRecords(now);
        long totalReadBytes = getTotalReadBytes(now);
        now.setLongCounter(TOTAL_READ_RECORDS, totalReadRecords);
        now.setLongCounter(TOTAL_READ_BYTES, totalReadBytes);
        now.setLongCounter(TOTAL_ERROR_RECORDS, getTotalErrorRecords(now));
        now.setLongCounter(TOTAL_ERROR_BYTES, getTotalErrorBytes(now));
        now.setLongCounter(WRITE_SUCCEED_RECORDS, getWriteSucceedRecords(now));
        now.setLongCounter(WRITE_SUCCEED_BYTES, getWriteSucceedBytes(now));

        long timeInterval = now.getTimestamp() - old.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        long bytesSpeed = (totalReadBytes - getTotalReadBytes(old)) / sec;
        long recordsSpeed = (totalReadRecords - getTotalReadRecords(old)) / sec;

        now.setLongCounter(BYTE_SPEED, bytesSpeed < 0 ? 0 : bytesSpeed);
        now.setLongCounter(RECORD_SPEED, recordsSpeed < 0 ? 0 : recordsSpeed);
        now.setDoubleCounter(PERCENTAGE, now.getLongCounter(STAGE) / (double) totalStage);

        if (old.getThrowable() != null) {
            now.setThrowable(old.getThrowable());
        }

        return now;
    }

    public static long getTotalReadRecords(Communication communication)
    {

        return communication.getLongCounter(READ_SUCCEED_RECORDS) + communication.getLongCounter(READ_FAILED_RECORDS);
    }

    public static long getTotalReadBytes(Communication communication)
    {
        return communication.getLongCounter(READ_SUCCEED_BYTES) + communication.getLongCounter(READ_FAILED_BYTES);
    }

    public static long getTotalErrorRecords(Communication communication)
    {
        return communication.getLongCounter(READ_FAILED_RECORDS) + communication.getLongCounter(WRITE_FAILED_RECORDS);
    }

    public static long getTotalErrorBytes(Communication communication)
    {
        return communication.getLongCounter(READ_FAILED_BYTES) + communication.getLongCounter(WRITE_FAILED_BYTES);
    }

    public static long getWriteSucceedRecords(Communication communication)
    {
        return communication.getLongCounter(WRITE_RECEIVED_RECORDS) - communication.getLongCounter(WRITE_FAILED_RECORDS);
    }

    public static long getWriteSucceedBytes(Communication communication)
    {
        return communication.getLongCounter(WRITE_RECEIVED_BYTES) - communication.getLongCounter(WRITE_FAILED_BYTES);
    }

    public static class Stringify
    {
        private final static DecimalFormat df = new DecimalFormat("0.00");

        private Stringify() {}

        public static String getSnapshot(Communication communication)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Total ");
            sb.append(getTotal(communication));
            sb.append(" | ");
            sb.append("Speed ");
            sb.append(getSpeed(communication));
            sb.append(" | ");
            sb.append("Error ");
            sb.append(getError(communication));
            sb.append(" | ");
            sb.append(" All Task WaitWriterTime ");
            sb.append(PerfTrace.unitTime(communication.getLongCounter(WAIT_WRITER_TIME)));
            sb.append(" | ");
            sb.append(" All Task WaitReaderTime ");
            sb.append(PerfTrace.unitTime(communication.getLongCounter(WAIT_READER_TIME)));
            sb.append(" | ");
            if (communication.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME) > 0
                    || communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                    || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                    || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
                sb.append("Transformer Success ");
                sb.append(String.format("%d records", communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS)));
                sb.append(" | ");
                sb.append("Transformer Error ");
                sb.append(String.format("%d records", communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS)));
                sb.append(" | ");
                sb.append("Transformer Filter ");
                sb.append(String.format("%d records", communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)));
                sb.append(" | ");
                sb.append("Transformer usedTime ");
                sb.append(PerfTrace.unitTime(communication.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME)));
                sb.append(" | ");
            }
            sb.append("Percentage ");
            sb.append(getPercentage(communication));
            return sb.toString();
        }

        private static String getTotal(Communication communication)
        {
            return String.format("%d records, %d bytes", communication.getLongCounter(TOTAL_READ_RECORDS), communication.getLongCounter(TOTAL_READ_BYTES));
        }

        private static String getSpeed(Communication communication)
        {
            return String.format("%s/s, %d records/s", StrUtil.stringify(communication.getLongCounter(BYTE_SPEED)), communication.getLongCounter(RECORD_SPEED));
        }

        private static String getError(Communication communication)
        {
            return String.format("%d records, %d bytes", communication.getLongCounter(TOTAL_ERROR_RECORDS), communication.getLongCounter(TOTAL_ERROR_BYTES));
        }

        private static String getPercentage(Communication communication)
        {
            return df.format(communication.getDoubleCounter(PERCENTAGE) * 100) + "%";
        }
    }
}
