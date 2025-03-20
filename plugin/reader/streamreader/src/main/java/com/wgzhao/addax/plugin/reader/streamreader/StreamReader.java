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
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

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
        private Configuration originalConfig;
        private static final List<String> validUnits = Arrays.asList("d", "day", "M", "month", "y", "year", "h", "hour", "m", "minute", "s", "second", "w", "week");

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            dealColumn(this.originalConfig);

            Long sliceRecordCount = this.originalConfig
                    .getLong(Key.SLICE_RECORD_COUNT);
            if (null == sliceRecordCount) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The item sliceRecordCount is required.");
            }
            else if (sliceRecordCount < 1) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The value of item sliceRecordCount must be greater than 0.");
            }
        }

        private void dealColumn(Configuration originalConfig)
        {
            List<JSONObject> columns = originalConfig.getList(Key.COLUMN,
                    JSONObject.class);
            if (null == columns || columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The item column is required.");
            }

            List<String> dealColumns = new ArrayList<>();
            for (JSONObject eachColumn : columns) {
                Configuration eachColumnConfig = Configuration.from(eachColumn);
                try {
                    this.parseMixupFunctions(eachColumnConfig);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            String.format("Failed to parse mixup functions [%s]", e.getMessage()), e);
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
                                NOT_SUPPORT_TYPE,
                                String.format("The [%s] is unsupported.", typeName));
                    }
                }
                dealColumns.add(eachColumnConfig.toJSON());
            }

            originalConfig.set(Key.COLUMN, dealColumns);
        }

        /**
         * Supports random functions, examples are as follows:
         * LONG: random 0, 10 - random number between 0 and 10
         * STRING: random 0, 10 - random string with length between 0 and 10
         * BOOL: random 0, 10 - ratio of false and true
         * DOUBLE: random 0, 10 - random floating-point number between 0 and 10
         * DOUBLE: random 0, 10, 2 - random floating-point number between 0 and 10 with 2 decimal places
         * DATE: random 2014-07-07 00:00:00, 2016-07-07 00:00:00 - random date between start and end time,
         * default date format (comma not supported) is yyyy-MM-dd HH:mm:ss
         * BYTES: random 0, 10 - random string with length between 0 and 10, encoded in UTF-8 binary
         * When a mixup function is configured, value can be omitted
         * If neither is configured
         * Supports increment functions, currently only supports integer types, examples are as follows:
         * LONG: incr 100 - starts from 100, increments by 1 each time
         * LONG: incr 0, 1 - starts from 0, increments by 1 each time
         * LONG: incr 1, 10 - starts from 1, increments by 10 each time
         * LONG: incr 1000, 1 - starts from 1000, increments by -1 each time, allowing negative values
         * DATE: incr &lt;date from&gt; &lt;interval&gt; &lt;unit&gt;
         * date from: specifies the start date, required
         * interval: interval period, default is 1, negative values decrement, optional
         * unit: interval unit, default is day, can be set to d/day, m/month, y/year
         *
         * @param eachColumnConfig see {@link Configuration}
         */
        private void parseMixupFunctions(Configuration eachColumnConfig)
        {
            String columnValue = eachColumnConfig.getString(Key.VALUE);
            String columnRandom = eachColumnConfig.getString(StreamConstant.RANDOM);
            String columnIncr = eachColumnConfig.getString(StreamConstant.INCR);
            if (StringUtils.isBlank(columnRandom) && StringUtils.isBlank(columnIncr)) {
                eachColumnConfig.getNecessaryValue(Key.VALUE, REQUIRED_VALUE);
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
                                ILLEGAL_VALUE,
                                "The value of  must be numeric, value [" + columnValue + "] is not valid."
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
                        }
                        catch (NumberFormatException e) {
                            throw AddaxException.asAddaxException(
                                    ILLEGAL_VALUE,
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
                            NOT_SUPPORT_TYPE,
                            "The increment sequence must be long or date, value [" + dType + "] is not valid."
                    );
                }
                this.originalConfig.set(StreamConstant.HAVE_INCR_FUNCTION, true);
            }
            // 三者都有配置
            if ((StringUtils.isNotBlank(columnRandom) || StringUtils.isNotBlank(columnIncr)) && StringUtils.isNotBlank(columnValue)) {
                LOG.warn("You both configure the constant column(value:{}) and random column(random:{}) " +
                                "or incr column(incr:{}), constant column is prior to others.",
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
                            ILLEGAL_VALUE,
                            String.format("Illegal random value [%s], supported random value like 'minVal, MaxVal[,scale]'",
                                    columnRandom));
                }
                String param1 = split[0];
                long param1Int;
                String param2 = split[1];
                long param2Int;
                if (StringUtils.isBlank(param1) && StringUtils.isBlank(param2)) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            "The random function's params can not be empty.");
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
                                ILLEGAL_VALUE,
                                String.format("The random function's params [%s,%s] does not match the dateFormat[%s].",
                                        dateFormat, param1, param2), e);
                    }
                }
                else {
                    param1Int = Integer.parseInt(param1);
                    param2Int = Integer.parseInt(param2);
                }
                if (param1Int < 0 || param2Int < 0) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            String.format("The random function's params [%s,%s] can not be negative.",
                                    param1, param2));
                }
                if (!Type.BOOL.name().equalsIgnoreCase(typeName) && param1Int > param2Int) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            String.format("The random function's params [%s,%s] is not valid, the first param must be less than the second one.",
                                    param1, param2));
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
         * 1. d/day
         * 2. M/month
         * 3. y/year
         * 4. h/hour
         * 5. m/minute
         * 6. s/second
         * 7. w/week
         *
         * @param unit the date interval unit
         */
        private void validateDateIncrUnit(String unit)
        {
            boolean isOK = true;
            if (unit.length() == 1) {
                if (!validUnits.contains(unit)) {
                    isOK = false;
                }
            }
            else if (!validUnits.contains(unit.toLowerCase())) {
                isOK = false;
            }
            if (!isOK) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
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
            int scale = eachColumnConfig.getInt(StreamConstant.MIXUP_FUNCTION_SCALE, -1);
            boolean isColumnMixup = StringUtils.isNotBlank(columnRandom);
            boolean isIncr = StringUtils.isNotBlank(columnIncr);
            UniformRandomProvider rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
            if (isColumnMixup) {
                switch (columnType) {
                    case STRING:
                        return new StringColumn(RandomStringUtils.randomAlphanumeric(
                                (int) rng.nextLong(param1Int, param2Int + 1)));
                    case LONG:
                        return new LongColumn(rng.nextLong(param1Int, param2Int + 1));
                    case DOUBLE:
                        // specify fixed scale or not ?
                        if (scale > 0) {
                            BigDecimal b = BigDecimal.valueOf(rng.nextDouble(param1Int, param2Int + 1))
                                    .setScale(scale, RoundingMode.HALF_UP);
                            return new DoubleColumn(b.doubleValue());
                        }
                        else {
                            return new DoubleColumn(rng.nextDouble(param1Int, param2Int + 1));
                        }
                    case DATE:
                        return new DateColumn(new Date(rng.nextLong(param1Int, param2Int + 1)));
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
                            long randomInt = rng.nextLong(0, param1Int + param2Int + 1);
                            return new BoolColumn(randomInt > param1Int);
                        }
                    case BYTES:
                        return new BytesColumn(RandomStringUtils.randomAlphanumeric((int)
                                rng.nextLong(param1Int, param2Int + 1)).getBytes());
                    case TIMESTAMP:
                        return new TimestampColumn(rng.nextLong(1_100_000_000_000L, 2_100_000_000_000L));
                    default:
                        // in fact,never to be here
                        throw new Exception("The type " + columnType.name() + "is not supported");
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
                }
                else if (columnType == Type.DATE) {
                    String[] fields = columnIncr.split(",");
                    currVal = incrMap.getOrDefault(columnIndex, null);
                    if (currVal == null) {
                        String datePattern = eachColumnConfig.getString(Key.DATE_FORMAT, StreamConstant.DEFAULT_DATE_FORMAT);
                        SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
                        try {
                            currVal = sdf.parse(fields[0]);
                        }
                        catch (java.text.ParseException e) {
                            throw AddaxException.asAddaxException(
                                    ILLEGAL_VALUE,
                                    String.format("can not parse date value [%s] with date format [%s]", fields[0], datePattern)
                            );
                        }
                    }
                    incrMap.put(columnIndex, dateIncrement((Date) currVal, Integer.parseInt(fields[1]), fields[2]));
                    return new DateColumn((Date) currVal);
                }
                else {
                    throw AddaxException.asAddaxException(
                            NOT_SUPPORT_TYPE,
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
                        throw new Exception(String.format("The column type [%s] is unsupported.", columnType.name()));
                }
            }
        }

        /**
         * calculate next date via interval
         *
         * @param curDate current date
         * @param step interval
         * @param unit unit
         * @return next date
         */
        private Date dateIncrement(Date curDate, int step, String unit)
        {
            switch (unit) {
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
                throw new IllegalArgumentException("The parameter recordSender must not be null.");
            }

            if (null == columns || columns.isEmpty()) {
                throw new IllegalArgumentException("The parameter columns must not be null or empty.");
            }

            Record record = recordSender.createRecord();
            try {
                for (int i = 0; i < columns.size(); i++) {
                    Configuration eachColumnConfig = Configuration.from(columns.get(i));
                    record.addColumn(this.buildOneColumn(eachColumnConfig, i));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "Failed to build record.", e);
            }
            return record;
        }
    }
}
