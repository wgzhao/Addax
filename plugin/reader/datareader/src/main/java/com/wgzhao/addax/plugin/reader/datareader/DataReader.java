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

package com.wgzhao.addax.plugin.reader.datareader;

import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.CaseFormat;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.plugin.reader.datareader.util.AddressUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.BankUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.CompanyUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.EmailUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.GeoUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.IdCardUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.JobUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.PersonUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.PhoneUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.StockUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_DATE_FORMAT;
import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.DATE_FORMAT;
import static com.wgzhao.addax.core.base.Key.SLICE_RECORD_COUNT;
import static com.wgzhao.addax.core.base.Key.TYPE;
import static com.wgzhao.addax.core.base.Key.VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.plugin.reader.datareader.DataKey.RULE;

public class DataReader
        extends Reader
{

    private enum Type
    {
        STRING, LONG, BOOL, DOUBLE, DATE, BYTES;

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
        private Configuration originalConfig;
        private static final List<String> validUnits = List.of("d", "day", "M", "month", "y", "year", "h", "hour", "m", "minute", "s", "second", "w", "week");

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            // Warn: Ignore case sensitivity
            dealColumn(this.originalConfig);

            Long sliceRecordCount = this.originalConfig.getLong(SLICE_RECORD_COUNT);
            if (sliceRecordCount == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "sliceRecordCount is required");
            }
            if (sliceRecordCount < 1) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "sliceRecordCount must be greater than 1");
            }
        }

        private void dealColumn(Configuration originalConfig)
        {
            List<JSONObject> columns = originalConfig.getList(COLUMN, JSONObject.class);
            if (columns == null || columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "column is required and must NOT be empty");
            }

            List<String> dealColumns = new ArrayList<>();
            for (JSONObject eachColumn : columns) {
                Configuration eachColumnConfig = Configuration.from(eachColumn);
                try {
                    this.parseMixupFunctions(eachColumnConfig);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                            String.format("Failed to parse column: %s", e.getMessage()), e);
                }
                String typeName = eachColumnConfig.getString(TYPE);
                if (StringUtils.isBlank(typeName)) {
                    // Empty typeName will be set to default type: string
                    eachColumnConfig.set(TYPE, Type.STRING);
                }
                else {
                    if (Type.DATE.name().equalsIgnoreCase(typeName)) {
                        boolean notAssignDateFormat = StringUtils.isBlank(eachColumnConfig.getString(DATE_FORMAT));
                        if (notAssignDateFormat) {
                            eachColumnConfig.set(DATE_FORMAT, DEFAULT_DATE_FORMAT);
                        }
                    }
                    if (!Type.isTypeIllegal(typeName)) {
                        throw AddaxException.asAddaxException(
                                NOT_SUPPORT_TYPE,
                                String.format("Unsupported type [%s]", typeName));
                    }
                }
                dealColumns.add(eachColumnConfig.toJSON());
            }

            originalConfig.set(COLUMN, dealColumns);
        }

        /**
         * Supports random functions, examples are as follows:
         * LONG: random 0, 10 - Random number between 0 and 10
         * STRING: random 0, 10 - Random string with a length between 0 and 10
         * BOOL: random 0, 10 - Ratio of false and true occurrences
         * DOUBLE: random 0, 10 - Random floating-point number between 0 and 10
         * DOUBLE: random 0, 10, 2 - Random floating-point number between 0 and 10 with 2 decimal places
         * DATE: random 2014-07-07 00:00:00, 2016-07-07 00:00:00 - Random time between the start and end time,
         * default date format (commas not supported): yyyy-MM-dd HH:mm:ss
         * BYTES: random 0, 10 - Random string with a length between 0 and 10, converted to its UTF-8 binary representation
         * <p>
         * If a mixup function is configured, the value can be omitted.
         * If neither is configured, an error will occur.
         * <p>
         * Additional parameters:
         * - date from: Specifies the start date (required)
         * - interval: Interval period, defaults to 1, negative values decrement (optional)
         * - unit: Interval unit, defaults to "day", can be set to d/day, m/month, y/year, etc.
         *
         * @param eachColumnConfig see {@link Configuration}
         */
        private void parseMixupFunctions(Configuration eachColumnConfig)
        {
            String columnRule = eachColumnConfig.getString(RULE, "constant").toLowerCase();
            switch (columnRule) {
                case "incr" -> validateIncrRule(eachColumnConfig);
                case "random" -> validateRandom(eachColumnConfig);
            }
        }

        /**
         * Supports incremental functions, currently only supports integer and date types. Examples are as follows:
         * <p>
         * LONG: incr 100 - Starts from 100, increments by 1 each time.
         * LONG: incr 0, 1 - Starts from 0, increments by 1 each time.
         * LONG: incr 1, 10 - Starts from 1, increments by 10 each time.
         * LONG: incr 1000, 1 - Starts from 1000, increments by -1 each time, allowing negative values.
         * DATE: incr <date from> <interval> <unit> - Starts from the specified date, increments by the given interval and unit.
         *
         * @param eachColumnConfig {@link Configuration} The configuration for the column.
         */
        private void validateIncrRule(Configuration eachColumnConfig)
        {
            String value = eachColumnConfig.getString(VALUE);
            String dType = eachColumnConfig.getString(TYPE).toLowerCase();
            switch (dType) {
                case "long" -> validateLongIncrRule(eachColumnConfig, value);
                case "date" -> validateDateIncrRule(eachColumnConfig, value);
                default -> throw AddaxException.asAddaxException(
                        NOT_SUPPORT_TYPE,
                        "Incremental sequence currently supports only LONG and DATE types"
                );
            }
        }

        private void validateLongIncrRule(Configuration eachColumnConfig, String value)
        {
            if (!value.contains(",")) {
                // Setup the default step value
                value = value + ",1";
                eachColumnConfig.set(VALUE, value);
            }
            try {
                Long.parseLong(value.split(",")[0].trim());
                Long.parseLong(value.split(",")[1].trim());
            }
            catch (NumberFormatException | IndexOutOfBoundsException e) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        value + " is illegal, it must be formatted as 'start, step'"
                );
            }
        }

        private void validateDateIncrRule(Configuration eachColumnConfig, String value)
        {
            String[] fields = value.split(",");
            if (fields.length == 1) {
                eachColumnConfig.set(VALUE, value.trim() + ",1,d");
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
                eachColumnConfig.set(VALUE, fields[0].trim() + "," + fields[1].trim() + ",d");
            }
            else {
                String unit = fields[2].charAt(0) + "";
                validateDateIncrUnit(unit);
                eachColumnConfig.set(VALUE, fields[0].trim() + "," + fields[1].trim() + "," + unit);
            }
        }

        private void validateRandom(Configuration config)
        {
            String value = config.getString(VALUE);
            String[] split = value.split(",");
            if (split.length < 2) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Illegal random value [%s], supported random value like 'minVal, MaxVal[,scale]'", value));
            }
            String param1 = split[0];
            long param1Int;
            String param2 = split[1];
            long param2Int;
            if (StringUtils.isBlank(param1) && StringUtils.isBlank(param2)) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        String.format("Random mixup function is invalid [%s], parameters cannot be empty: %s, %s",
                                value, param1, param2));
            }

            String typeName = config.getString(TYPE);
            if (Type.DATE.name().equalsIgnoreCase(typeName)) {
                String dateFormat = config.getString(DATE_FORMAT, DEFAULT_DATE_FORMAT);
                try {
                    SimpleDateFormat format = new SimpleDateFormat(config.getString(DATE_FORMAT, DEFAULT_DATE_FORMAT));
                    //warn: do no concern int -> long
                    param1Int = format.parse(param1).getTime();//milliseconds
                    param2Int = format.parse(param2).getTime();//milliseconds
                }
                catch (ParseException e) {
                    throw AddaxException.asAddaxException(
                            ILLEGAL_VALUE,
                            String.format("dateFormat parameter [%s] and random mixup function parameters do not match: %s, %s",
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
                        String.format("Random function is invalid [%s], parameters cannot be negative: %s, %s",
                                value, param1, param2));
            }
            if (!Type.BOOL.name().equalsIgnoreCase(typeName) && param1Int > param2Int) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        String.format("Random function is invalid [%s], the first parameter must be less than or equal to the second: %s, %s",
                                value, param1, param2));
            }
            config.set(DataKey.MIX_FUNCTION_PARAM1, param1Int);
            config.set(DataKey.MIX_FUNCTION_PARAM2, param2Int);
            if (split.length == 3) {
                int scale = Integer.parseInt(split[2].trim());
                config.set(DataKey.MIX_FUNCTION_SCALE, scale);
            }
//                this.originalConfig.set(DataConstant.HAVE_MIXUP_FUNCTION, true);
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

            if (!validUnits.contains(unit) && !validUnits.contains(unit.toLowerCase())) {
                throw AddaxException.asAddaxException(
                        ILLEGAL_VALUE,
                        unit + " is NOT a valid interval unit. Refer to the documentation for supported units."
                );
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

        // 递增字段字段，用于存储当前的递增值
        private static final Map<Integer, Object> incrMap = new HashMap<>();

        @Override
        public void init()
        {
            Configuration readerSliceConfig = getPluginJobConf();
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);
            this.sliceRecordCount = readerSliceConfig.getLong(Key.SLICE_RECORD_COUNT);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            Record oneRecord;
            while (this.sliceRecordCount > 0) {
                oneRecord = buildOneRecord(recordSender, this.columns);
                recordSender.sendToWriter(oneRecord);
                this.sliceRecordCount--;
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
            String columnValue = eachColumnConfig.getString(VALUE);
            // convert rule from caseFormat to UPPER_UNDERSCORE
            String rule = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, eachColumnConfig.getString(RULE, "constant"));
            Type columnType = Type.valueOf(eachColumnConfig.getString(TYPE, "string").toUpperCase());
            Rule columnRule = Rule.valueOf(rule);
            return switch (columnRule) {
                case CONSTANT -> buildConstantColumn(columnValue, columnType, eachColumnConfig);
                case RANDOM -> buildRandomColumn(columnValue, columnType, eachColumnConfig);
                case INCR -> buildIncrementalColumn(columnValue, columnType, eachColumnConfig, columnIndex);
                default -> buildOtherColumn(columnRule, columnType);
            };
        }

        private Column buildConstantColumn(String columnValue, Type columnType, Configuration eachColumnConfig)
                throws Exception
        {
            if ("null".equals(columnValue)) {
                return null;
            }
            return switch (columnType) {
                case STRING -> new StringColumn(columnValue);
                case LONG -> new LongColumn(columnValue);
                case DOUBLE -> new DoubleColumn(columnValue);
                case DATE -> {
                    SimpleDateFormat format = new SimpleDateFormat(eachColumnConfig.getString(DATE_FORMAT, DEFAULT_DATE_FORMAT));
                    yield new DateColumn(format.parse(columnValue));
                }
                case BOOL -> new BoolColumn("true".equalsIgnoreCase(columnValue));
                case BYTES -> new BytesColumn(columnValue.getBytes());
            };
        }

        private Column buildRandomColumn(String columnValue, Type columnType, Configuration eachColumnConfig)
                throws Exception
        {
            if ("null".equals(columnValue)) {
                return null;
            }
            long param1Int = eachColumnConfig.getLong(DataKey.MIX_FUNCTION_PARAM1, 0L);
            long param2Int = eachColumnConfig.getLong(DataKey.MIX_FUNCTION_PARAM2, 1L);
            int scale = eachColumnConfig.getInt(DataKey.MIX_FUNCTION_SCALE, -1);
            UniformRandomProvider rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
            return switch (columnType) {
                case STRING -> new StringColumn(RandomStringUtils.randomAlphanumeric(
                        (int) rng.nextLong(param1Int, param2Int + 1)));
                case LONG -> new LongColumn(rng.nextLong(param1Int, param2Int + 1));
                case DOUBLE -> {
                    if (scale > 0) {
                        BigDecimal b = BigDecimal.valueOf(rng.nextDouble(param1Int, param2Int + 1))
                                .setScale(scale, RoundingMode.HALF_UP);
                        yield new DoubleColumn(b.doubleValue());
                    }
                    else {
                        yield new DoubleColumn(rng.nextDouble(param1Int, param2Int + 1));
                    }
                }
                case DATE -> new DateColumn(new Date(rng.nextLong(param1Int, param2Int + 1)));
                case BOOL -> {
                    if (param1Int == param2Int) {
                        param1Int = 0;
                        param2Int = 1;
                    }
                    if (param1Int == 0) {
                        yield new BoolColumn(true);
                    }
                    else if (param2Int == 0) {
                        yield new BoolColumn(false);
                    }
                    else {
                        long randomInt = rng.nextLong(0, param1Int + param2Int + 1);
                        yield new BoolColumn(randomInt > param1Int);
                    }
                }
                case BYTES -> new BytesColumn(RandomStringUtils.randomAlphanumeric((int)
                        rng.nextLong(param1Int, param2Int + 1)).getBytes());
            };
        }

        private Column buildIncrementalColumn(String columnValue, Type columnType, Configuration eachColumnConfig, int columnIndex)
                throws Exception
        {
            Object currVal;
            long step;
            if (columnType == Type.LONG) {
                currVal = Long.parseLong(columnValue.split(",")[0]);
                step = Long.parseLong(columnValue.split(",")[1]);
                currVal = incrMap.getOrDefault(columnIndex, currVal);
                incrMap.put(columnIndex, (long) currVal + step);
                return new LongColumn((long) currVal);
            }
            else if (columnType == Type.DATE) {
                String[] fields = columnValue.split(",");
                currVal = incrMap.getOrDefault(columnIndex, null);
                if (currVal == null) {
                    String datePattern = eachColumnConfig.getString(DATE_FORMAT, DEFAULT_DATE_FORMAT);
                    SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
                    try {
                        currVal = sdf.parse(fields[0]);
                    }
                    catch (java.text.ParseException e) {
                        throw AddaxException.asAddaxException(
                                ILLEGAL_VALUE,
                                String.format("Cannot parse date value [%s] with date format [%s]", fields[0], datePattern)
                        );
                    }
                }
                incrMap.put(columnIndex, dateIncrement((Date) currVal, Integer.parseInt(fields[1]), fields[2]));
                return new DateColumn((Date) currVal);
            }
            else {
                throw AddaxException.asAddaxException(
                        NOT_SUPPORT_TYPE,
                        columnType + " cannot support increment"
                );
            }
        }

        private Column buildOtherColumn(Rule columnRule, Type columnType)
                throws Exception
        {
            return switch (columnRule) {
                case ADDRESS -> new StringColumn(AddressUtil.nextAddress());
                case BANK -> new StringColumn(BankUtil.nextBank());
                case DEBIT_CARD -> new StringColumn(BankUtil.nextDebitCard());
                case CREDIT_CARD -> new StringColumn(BankUtil.nextCreditCard());
                case COMPANY -> new StringColumn(CompanyUtil.nextCompany());
                case EMAIL -> new StringColumn(EmailUtil.nextEmail());
                case ID_CARD -> new StringColumn(IdCardUtil.nextIdCard());
                case LAT, LATITUDE -> new DoubleColumn(GeoUtil.latitude().doubleValue());
                case LNG, LONGITUDE -> new DoubleColumn(GeoUtil.longitude().doubleValue());
                case JOB -> new StringColumn(JobUtil.nextJob());
                case PHONE -> new StringColumn(PhoneUtil.nextPhoneNumber());
                case STOCK_CODE -> new StringColumn(StockUtil.nextStockCode());
                case STOCK_ACCOUNT -> new StringColumn(StockUtil.nextStockAccount());
                case NAME -> new StringColumn(PersonUtil.nextName());
                case UUID -> new StringColumn(UUID.randomUUID().toString());
                case ZIP_CODE -> new LongColumn(RandomSource.XO_RO_SHI_RO_1024_PP.create().nextLong(1000000, 699000));
                default -> throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        columnRule + " is unsupported");
            };
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
            return switch (unit) {
                case "d" -> DateUtils.addDays(curDate, step);
                case "M" -> DateUtils.addMonths(curDate, step);
                case "y" -> DateUtils.addYears(curDate, step);
                case "w" -> DateUtils.addWeeks(curDate, step);
                case "h" -> DateUtils.addHours(curDate, step);
                case "m" -> DateUtils.addMinutes(curDate, step);
                case "s" -> DateUtils.addSeconds(curDate, step);
                default -> throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The date interval unit '" + unit + "' is unsupported");
            };
        }

        private Record buildOneRecord(RecordSender recordSender,
                List<String> columns)
        {
            if (recordSender == null) {
                throw new IllegalArgumentException("Parameter [recordSender] cannot be null.");
            }

            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("Parameter [column] cannot be null or empty.");
            }

            Record record = recordSender.createRecord();
            try {
                for (int i = 0; i < columns.size(); i++) {
                    Configuration eachColumnConfig = Configuration.from(columns.get(i));
                    record.addColumn(this.buildOneColumn(eachColumnConfig, i));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, "Failed to construct a record.", e);
            }
            return record;
        }
    }
}
