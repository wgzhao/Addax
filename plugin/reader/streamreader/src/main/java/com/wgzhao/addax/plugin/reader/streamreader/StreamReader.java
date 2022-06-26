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

package com.wgzhao.addax.plugin.reader.streamreader;

import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.element.TimestampColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class StreamReader
        extends Reader
{

    private enum Type
    {
        STRING, LONG, BOOL, DOUBLE, DATE, BYTES, TIMESTAMP,
        ;

        private static boolean isTypeIllegal(String typeString)
        {
            try {
                Type.valueOf(typeString.toUpperCase());
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }
    }

    public static class Job
            extends Reader.Job
    {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Pattern mixupFunctionPattern;
        private Configuration originalConfig;
        private static final List<String> validUnits = Arrays.asList("d", "day", "M", "month", "y", "year", "h", "hour", "m", "minute", "s", "second", "w", "week");

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            // warn: 忽略大小写
            this.mixupFunctionPattern = Pattern.compile(StreamConstant.MIXUP_FUNCTION_PATTERN,
                    Pattern.CASE_INSENSITIVE);
            dealColumn(this.originalConfig);

            Long sliceRecordCount = this.originalConfig
                    .getLong(Key.SLICE_RECORD_COUNT);
            if (null == sliceRecordCount) {
                throw AddaxException.asAddaxException(StreamReaderErrorCode.REQUIRED_VALUE,
                        "没有设置参数[sliceRecordCount].");
            }
            else if (sliceRecordCount < 1) {
                throw AddaxException.asAddaxException(StreamReaderErrorCode.ILLEGAL_VALUE,
                        "参数[sliceRecordCount]不能小于1.");
            }
        }

        private void dealColumn(Configuration originalConfig)
        {
            List<JSONObject> columns = originalConfig.getList(Key.COLUMN,
                    JSONObject.class);
            if (null == columns || columns.isEmpty()) {
                throw AddaxException.asAddaxException(StreamReaderErrorCode.REQUIRED_VALUE,
                        "没有设置参数[column].");
            }

            List<String> dealColumns = new ArrayList<>();
            for (JSONObject eachColumn : columns) {
                Configuration eachColumnConfig = Configuration.from(eachColumn);
                try {
                    this.parseMixupFunctions(eachColumnConfig);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(StreamReaderErrorCode.NOT_SUPPORT_TYPE,
                            String.format("解析混淆函数失败[%s]", e.getMessage()), e);
                }

                String typeName = eachColumnConfig.getString(Key.TYPE);
                if (StringUtils.isBlank(typeName)) {
                    // empty typeName will be set to default type: string
                    eachColumnConfig.set(Key.TYPE, Type.STRING);
                }
                else {
                    if (Type.DATE.name().equalsIgnoreCase(typeName)) {
                        boolean notAssignDateFormat = StringUtils
                                .isBlank(eachColumnConfig.getString(Key.DATE_FORMAT));
                        if (notAssignDateFormat) {
                            eachColumnConfig.set(Key.DATE_FORMAT, StreamConstant.DEFAULT_DATE_FORMAT);
                        }
                    }
                    if (!Type.isTypeIllegal(typeName)) {
                        throw AddaxException.asAddaxException(
                                StreamReaderErrorCode.NOT_SUPPORT_TYPE,
                                String.format("不支持类型[%s]", typeName));
                    }
                }
                dealColumns.add(eachColumnConfig.toJSON());
            }

            originalConfig.set(Key.COLUMN, dealColumns);
        }

        /**
         * 支持随机函数, demo如下:
         * LONG: random 0, 10 0到10之间的随机数字
         * STRING: random 0, 10 0到10长度之间的随机字符串
         * BOOL: random 0, 10 false 和 true出现的比率
         * DOUBLE: random 0, 10 0到10之间的随机浮点数
         * DOUBLE: random 0, 10, 2 0到10之间的随机浮点数，小数位为2位
         * DATE: random 2014-07-07 00:00:00, 2016-07-07 00:00:00 开始时间-&gt;结束时间之间的随机时间，
         * 日期格式默认(不支持逗号)yyyy-MM-dd HH:mm:ss
         * BYTES: random 0, 10 0到10长度之间的随机字符串获取其UTF-8编码的二进制串
         * 配置了混淆函数后，可不配置value
         * 2者都没有配置
         * 支持递增函数，当前仅支持整数类型，demo如下
         * LONG: incr 100 从100开始，每次加1
         * LONG: incr 0, 1 从0开始，每次加1
         * LONG: incr 1, 10 从 1 开始，每次加10
         * LONG: incr 1000, 1 从 1000开始，每次加-1，允许出现负数
         * DATE: incr &lt;date from&gt; &lt;interval&gt; &lt;unit&gt;
         * date from : 指定开始日期，必填
         * interval: 隔间周期，默认为1，负数则递减 选填
         * unit: 间隔单位，默认为day，可设置为 d/day, m/month, y/year
         *
         * @param eachColumnConfig see {@link Configuration}
         */
        private void parseMixupFunctions(Configuration eachColumnConfig)
        {
            String columnValue = eachColumnConfig.getString(Key.VALUE);
            String columnRandom = eachColumnConfig.getString(StreamConstant.RANDOM);
            String columnIncr = eachColumnConfig.getString(StreamConstant.INCR);
            if (StringUtils.isBlank(columnRandom) && StringUtils.isBlank(columnIncr)) {
                eachColumnConfig.getNecessaryValue(Key.VALUE, StreamReaderErrorCode.REQUIRED_VALUE);
            }
            if (StringUtils.isNotBlank(columnIncr)) {
                // 类型判断
                String dType = eachColumnConfig.getString(Key.TYPE).toLowerCase();
                if ("long".equals(dType)) {
                    //  columnValue is valid number ?
                    if (!columnIncr.contains(",")) {
                        // setup the default step value
                        columnIncr = columnIncr + ",1";
                        eachColumnConfig.set(StreamConstant.INCR, columnIncr);
                    }
                    // validate value
                    try {
                        Long.parseLong(columnIncr.split(",")[0].trim());
                        Long.parseLong(columnIncr.split(",")[1].trim());
                    }
                    catch (NumberFormatException e) {
                        throw AddaxException.asAddaxException(
                                StreamReaderErrorCode.ILLEGAL_VALUE,
                                columnValue + " 不是合法的数字字符串"
                        );
                    }
                }
                else if ("date".equals(dType)) {
                    String[] fields = columnIncr.split(",");
                    if (fields.length == 1) {
                        eachColumnConfig.set(StreamConstant.INCR, columnIncr.trim() + ",1,d");
                    }
                    else if (fields.length == 2) {
                        try {
                            Integer.parseInt(fields[1]);
                        } catch (NumberFormatException e) {
                            throw AddaxException.asAddaxException(
                                    StreamReaderErrorCode.ILLEGAL_VALUE,
                                    "The second field must be numeric, value [" + fields[1] + "] is not valid"
                            );
                        }
                        eachColumnConfig.set(StreamConstant.INCR, fields[0].trim() + "," + fields[1].trim() + ",d");
                    }
                    else {
                        String unit = fields[2].charAt(0) + "";
                        // validate unit
                        validateDateIncrUnit(unit);
                        // normalize unit to 1-char
                        eachColumnConfig.set(StreamConstant.INCR, fields[0].trim() + "," + fields[1].trim() + "," + unit);
                    }
                }
                else {
                    throw AddaxException.asAddaxException(
                            StreamReaderErrorCode.NOT_SUPPORT_TYPE,
                            "递增序列当前仅支持整数类型(long)和日期类型(date)"
                    );
                }
                this.originalConfig.set(StreamConstant.HAVE_INCR_FUNCTION, true);
            }
            // 三者都有配置
            if ((StringUtils.isNotBlank(columnRandom) || StringUtils.isNotBlank(columnIncr)) && StringUtils.isNotBlank(columnValue)) {
                LOG.warn("您配置了streamreader常量列(value:{})和随机混淆列(random:{})或递增列(incr:{}), 常量列优先",
                        columnValue, columnRandom, columnIncr);
                if (StringUtils.isNotBlank(columnRandom)) {
                    eachColumnConfig.remove(StreamConstant.RANDOM);
                }
                if (StringUtils.isNotBlank(columnIncr)) {
                    eachColumnConfig.remove(StreamConstant.INCR);
                }
            }
            if (StringUtils.isNotBlank(columnRandom)) {
                String[] split = columnRandom.split(",");
                if (split.length < 2) {
                    throw AddaxException.asAddaxException(
                            StreamReaderErrorCode.ILLEGAL_VALUE,
                            String.format("Illegal random value [%s], supported random value like 'minVal, MaxVal[,scale]'",
                                    columnRandom));
                }
                String param1 = split[0];
                long param1Int;
                String param2 = split[1];
                long param2Int;
                if (StringUtils.isBlank(param1) && StringUtils.isBlank(param2)) {
                    throw AddaxException.asAddaxException(
                            StreamReaderErrorCode.ILLEGAL_VALUE,
                            String.format("random混淆函数不合法[%s], 混淆函数random的参数不能为空:%s, %s",
                                    columnRandom, param1, param2));
                }

                String typeName = eachColumnConfig.getString(Key.TYPE);
                if (Type.DATE.name().equalsIgnoreCase(typeName)) {
                    String dateFormat = eachColumnConfig.getString(Key.DATE_FORMAT, StreamConstant.DEFAULT_DATE_FORMAT);
                    try {
                        SimpleDateFormat format = new SimpleDateFormat(
                                eachColumnConfig.getString(Key.DATE_FORMAT, StreamConstant.DEFAULT_DATE_FORMAT));
                        //warn: do no concern int -> long
                        param1Int = format.parse(param1).getTime();//milliseconds
                        param2Int = format.parse(param2).getTime();//milliseconds
                    }
                    catch (ParseException e) {
                        throw AddaxException.asAddaxException(
                                StreamReaderErrorCode.ILLEGAL_VALUE,
                                String.format("dateFormat参数[%s]和混淆函数random的参数不匹配，解析错误:%s, %s",
                                        dateFormat, param1, param2), e);
                    }
                }
                else {
                    param1Int = Integer.parseInt(param1);
                    param2Int = Integer.parseInt(param2);
                }
                if (param1Int < 0 || param2Int < 0) {
                    throw AddaxException.asAddaxException(
                            StreamReaderErrorCode.ILLEGAL_VALUE,
                            String.format("random混淆函数不合法[%s], 混淆函数random的参数不能为负数:%s, %s",
                                    columnRandom, param1, param2));
                }
                if (!Type.BOOL.name().equalsIgnoreCase(typeName) && param1Int > param2Int) {
                    throw AddaxException.asAddaxException(
                            StreamReaderErrorCode.ILLEGAL_VALUE,
                            String.format("random混淆函数不合法[%s], 混淆函数random的参数需要第一个小于等于第二个:%s, %s",
                                    columnRandom, param1, param2));
                }
                eachColumnConfig.set(StreamConstant.MIXUP_FUNCTION_PARAM1, param1Int);
                eachColumnConfig.set(StreamConstant.MIXUP_FUNCTION_PARAM2, param2Int);
                if (split.length == 3) {
                    int scale = Integer.parseInt(split[2].trim());
                    eachColumnConfig.set(StreamConstant.MIXUP_FUNCTION_SCALE, scale);
                }
                this.originalConfig.set(StreamConstant.HAVE_MIXUP_FUNCTION, true);
            }
        }

        /**
         * valid the unit
         * current support unit are the following:
         *  1. d/day
         *  2. M/month
         *  3. y/year
         *  4. h/hour
         *  5. m/minute
         *  6. s/second
         *  7. w/week
         * @param unit the date interval unit
         */
        private void validateDateIncrUnit(String unit)
        {
            boolean isOK = true;
            if ( unit.length() == 1 ) {
                if (! validUnits.contains(unit)) {
                    isOK = false;
                }
            }  else if (! validUnits.contains(unit.toLowerCase())) {
                isOK = false;
            }
            if (!isOK) {
                throw AddaxException.asAddaxException(
                        StreamReaderErrorCode.ILLEGAL_VALUE,
                        unit + " is NOT valid interval unit，for more details, please refer to the documentation");
            }
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> configurations = new ArrayList<>();

            for (int i = 0; i < adviceNumber; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    public static class Task
            extends Reader.Task
    {
        private List<String> columns;

        private long sliceRecordCount;

        private boolean haveMixupFunction;
        private boolean haveIncrFunction;

        // 递增字段字段，用于存储当前的递增值
        private static final Map<Integer, Object> incrMap = new ConcurrentHashMap<>(8);

        @Override
        public void init()
        {
            Configuration readerSliceConfig = getPluginJobConf();
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);

            this.sliceRecordCount = readerSliceConfig.getLong(Key.SLICE_RECORD_COUNT);
            this.haveMixupFunction = readerSliceConfig.getBool(StreamConstant.HAVE_MIXUP_FUNCTION, false);
            this.haveIncrFunction = readerSliceConfig.getBool(StreamConstant.HAVE_INCR_FUNCTION, false);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            Record oneRecord = buildOneRecord(recordSender, this.columns);
            while (this.sliceRecordCount > 0) {
                recordSender.sendToWriter(oneRecord);
                this.sliceRecordCount--;
                if (this.haveMixupFunction || this.haveIncrFunction) {
                    oneRecord = buildOneRecord(recordSender, this.columns);
                }
            }
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }

        private Column buildOneColumn(Configuration eachColumnConfig, int columnIndex)
                throws Exception
        {
            String columnValue = eachColumnConfig.getString(Key.VALUE);
            if ("null".equals(columnValue)) {
                return new StringColumn();
            }
            Type columnType = Type.valueOf(eachColumnConfig.getString(Key.TYPE).toUpperCase());
            String columnRandom = eachColumnConfig.getString(StreamConstant.RANDOM);
            String columnIncr = eachColumnConfig.getString(StreamConstant.INCR);
            long param1Int = eachColumnConfig.getLong(StreamConstant.MIXUP_FUNCTION_PARAM1, 0L);
            long param2Int = eachColumnConfig.getLong(StreamConstant.MIXUP_FUNCTION_PARAM2, 1L);
            int  scale = eachColumnConfig.getInt(StreamConstant.MIXUP_FUNCTION_SCALE, -1);
            boolean isColumnMixup = StringUtils.isNotBlank(columnRandom);
            boolean isIncr = StringUtils.isNotBlank(columnIncr);
            if (isColumnMixup) {
                switch (columnType) {
                    case STRING:
                        return new StringColumn(RandomStringUtils.randomAlphanumeric(
                                (int) RandomUtils.nextLong(param1Int, param2Int + 1)));
                    case LONG:
                        return new LongColumn(RandomUtils.nextLong(param1Int, param2Int + 1));
                    case DOUBLE:
                        // specify fixed scale or not ?
                        if (scale > 0) {
                            BigDecimal b = BigDecimal.valueOf(RandomUtils.nextDouble(param1Int, param2Int + 1))
                                    .setScale(scale, BigDecimal.ROUND_HALF_UP);
                            return new DoubleColumn(b.doubleValue());
                        } else {
                            return new DoubleColumn(RandomUtils.nextDouble(param1Int, param2Int + 1));
                        }
                    case DATE:
                        return new DateColumn(new Date(RandomUtils.nextLong(param1Int, param2Int + 1)));
                    case BOOL:
                        // warn: no concern -10 etc..., how about (0, 0)(0, 1)(1,2)
                        if (param1Int == param2Int) {
                            param1Int = 0;
                            param2Int = 1;
                        }
                        if (param1Int == 0) {
                            return new BoolColumn(true);
                        }
                        else if (param2Int == 0) {
                            return new BoolColumn(false);
                        }
                        else {
                            long randomInt = RandomUtils.nextLong(0, param1Int + param2Int + 1);
                            return new BoolColumn(randomInt > param1Int);
                        }
                    case BYTES:
                        return new BytesColumn(RandomStringUtils.randomAlphanumeric((int)
                                RandomUtils.nextLong(param1Int, param2Int + 1)).getBytes());
                    case TIMESTAMP:
                        return new TimestampColumn(RandomUtils.nextLong(1_100_000_000_000L, 2_100_000_000_000L));
                    default:
                        // in fact,never to be here
                        throw new Exception(String.format("不支持类型[%s]", columnType.name()));
                }
            }
            else if (isIncr) {
                Object currVal;
                long step;
                if (columnType == Type.LONG) {
                    //get initial value and step
                    currVal = Long.parseLong(columnIncr.split(",")[0]);
                    step = Long.parseLong(columnIncr.split(",")[1]);
                    currVal = incrMap.getOrDefault(columnIndex, currVal);
                    incrMap.put(columnIndex, (long) currVal + step);
                    return new LongColumn((long) currVal);
                } else if (columnType == Type.DATE) {
                    String[] fields = columnIncr.split(",");
                    currVal = incrMap.getOrDefault(columnIndex, null);
                    if (currVal == null) {
                        String datePattern = eachColumnConfig.getString(Key.DATE_FORMAT, StreamConstant.DEFAULT_DATE_FORMAT);
                        SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
                        try {
                            currVal = sdf.parse(fields[0]);
                        } catch (java.text.ParseException e) {
                            throw AddaxException.asAddaxException(
                                    StreamReaderErrorCode.ILLEGAL_VALUE,
                                    String.format("can not parse date value [%s] with date format [%s]", fields[0], datePattern)
                            );
                        }
                    }
                    incrMap.put(columnIndex, dateIncrement((Date) currVal, Integer.parseInt(fields[1]), fields[2]));
                    return new DateColumn((Date)currVal);
                } else {
                    throw AddaxException.asAddaxException(
                            StreamReaderErrorCode.NOT_SUPPORT_TYPE,
                            columnType + " can not support for increment"
                    );
                }
            }
            else {
                switch (columnType) {
                    case STRING:
                        return new StringColumn(columnValue);
                    case LONG:
                        return new LongColumn(columnValue);
                    case DOUBLE:
                        return new DoubleColumn(columnValue);
                    case DATE:
                        SimpleDateFormat format = new SimpleDateFormat(eachColumnConfig.getString(Key.DATE_FORMAT, StreamConstant.DEFAULT_DATE_FORMAT));
                        return new DateColumn(format.parse(columnValue));
                    case BOOL:
                        return new BoolColumn("true".equalsIgnoreCase(columnValue));
                    case BYTES:
                        return new BytesColumn(columnValue.getBytes());
                    case TIMESTAMP:
                        return new TimestampColumn(columnValue);
                    default:
                        // in fact,never to be here
                        throw new Exception(String.format("不支持类型[%s]",
                                columnType.name()));
                }
            }
        }

        /**
         * calculate next date via interval
         * @param curDate current date
         * @param step interval
         * @param unit unit
         * @return next date
         */
        private Date dateIncrement(Date curDate, int step, String unit)
        {
            switch(unit) {
                case "d":
                    return DateUtils.addDays(curDate, step);
                case "M":
                    return DateUtils.addMonths(curDate, step);
                case "y":
                    return DateUtils.addYears(curDate, step);
                case "w":
                    return DateUtils.addWeeks(curDate, step);
                case "h":
                    return DateUtils.addHours(curDate, step);
                case "m":
                    return DateUtils.addMinutes(curDate, step);
                case "s":
                    return DateUtils.addSeconds(curDate, step);
                default:
                    return DateUtils.addDays(curDate, step);
            }
        }

        private Record buildOneRecord(RecordSender recordSender,
                List<String> columns)
        {
            if (null == recordSender) {
                throw new IllegalArgumentException(
                        "参数[recordSender]不能为空.");
            }

            if (null == columns || columns.isEmpty()) {
                throw new IllegalArgumentException(
                        "参数[column]不能为空.");
            }

            Record record = recordSender.createRecord();
            try {
                for (int i = 0; i < columns.size(); i++) {
                    Configuration eachColumnConfig = Configuration.from(columns.get(i));
                    record.addColumn(this.buildOneColumn(eachColumnConfig, i));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(StreamReaderErrorCode.ILLEGAL_VALUE,
                        "构造一个record失败.", e);
            }
            return record;
        }
    }
}
