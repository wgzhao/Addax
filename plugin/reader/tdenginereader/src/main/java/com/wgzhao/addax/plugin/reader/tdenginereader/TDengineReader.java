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

package com.wgzhao.addax.plugin.reader.tdenginereader;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.rdbms.reader.CommonRdbmsReader;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_DATE_FORMAT;
import static com.wgzhao.addax.common.base.Constant.DEFAULT_FETCH_SIZE;
import static com.wgzhao.addax.common.base.Key.FETCH_SIZE;

public class TDengineReader
        extends Reader
{

    private static final DataBaseType DATABASE_TYPE = DataBaseType.TDengine;

    public static class Job
            extends Reader.Job
    {

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck()
        {
            this.commonRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);

            SimpleDateFormat format = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
            // check beginDateTime
            String beginDatetime = this.originalConfig.getString(TDKey.BEGIN_DATETIME);
            if (StringUtils.isBlank(beginDatetime)) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + TDKey.BEGIN_DATETIME + "] is not set.");
            }
            long start;
            try {
                start = format.parse(beginDatetime).getTime();
            }
            catch (ParseException e) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + TDKey.BEGIN_DATETIME +
                        "] needs to conform to the [" + DEFAULT_DATE_FORMAT + "] format.");
            }

            // check endDateTime
            String endDatetime = this.originalConfig.getString(TDKey.END_DATETIME);
            if (StringUtils.isBlank(endDatetime)) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + TDKey.END_DATETIME + "] is not set.");
            }
            long end;
            try {
                end = format.parse(endDatetime).getTime();
            }
            catch (ParseException e) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + TDKey.END_DATETIME + "] " +
                        "needs to conform to the [" + DEFAULT_DATE_FORMAT + "] format.");
            }
            if (start >= end) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + TDKey.BEGIN_DATETIME +
                        "] should be less than the parameter [" + TDKey.END_DATETIME + "].");
            }

            // check splitInterval
            String splitInterval = this.originalConfig.getString(TDKey.SPLIT_INTERVAL);
            Long split;
            if (StringUtils.isBlank(splitInterval)) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.REQUIRED_VALUE, "The parameter [" + TDKey.SPLIT_INTERVAL +
                        "] is not set.");
            }
            try {
                split = parseSplitInterval(splitInterval);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(TDengineReaderErrorCode.ILLEGAL_VALUE, "The parameter [" + TDKey.SPLIT_INTERVAL +
                        "] should be like: \"123d|h|m|s\", error: " + e.getMessage());
            }

            this.originalConfig.set(TDKey.BEGIN_DATETIME, start);
            this.originalConfig.set(TDKey.END_DATETIME, end);
            this.originalConfig.set(TDKey.SPLIT_INTERVAL, split);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> configurations = new ArrayList<>();
            // do split
            Long start = this.originalConfig.getLong(TDKey.BEGIN_DATETIME);
            Long end = this.originalConfig.getLong(TDKey.END_DATETIME);
            Long split = this.originalConfig.getLong(TDKey.SPLIT_INTERVAL);
            
            for (Long ts = start; ts < end; ts += split) {
                Configuration clone = this.originalConfig.clone();
                clone.remove(TDKey.SPLIT_INTERVAL);

                clone.set(TDKey.BEGIN_DATETIME, ts);
                clone.set(TDKey.END_DATETIME, Math.min(ts + split, end));
                configurations.add(clone);
            }
            return configurations;
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }
    }

    public static class Task
            extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            // TDengine does not support fetch size
            int fetchSize = this.readerSliceConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }
    }

    private static final long second = 1000;
    private static final long minute = 60 * second;
    private static final long hour = 60 * minute;
    private static final long day = 24 * hour;

    private static Long parseSplitInterval(String splitInterval)
            throws Exception
    {
        Pattern compile = Pattern.compile("^(\\d+)([dhms])$");
        Matcher matcher = compile.matcher(splitInterval);
        while (matcher.find()) {
            Long value = Long.valueOf(matcher.group(1));
            if (value == 0) {
                throw new Exception("invalid splitInterval: 0");
            }
            char unit = matcher.group(2).charAt(0);
            switch (unit) {
                case 'd':
                    return value * day;
                default:
                case 'h':
                    return value * hour;
                case 'm':
                    return value * minute;
                case 's':
                    return value * second;
            }
        }
        throw new Exception("invalid splitInterval: " + splitInterval);
    }
}
