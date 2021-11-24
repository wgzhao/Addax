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

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.BytesColumn;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.plugin.reader.datareader.util.AddressUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.BankUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.CompanyUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.GeoUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.IdCardUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.JobUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.PersonUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.PhoneUtil;
import com.wgzhao.addax.plugin.reader.datareader.util.StockUtil;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_DATE_FORMAT;
import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.DATE_FORMAT;
import static com.wgzhao.addax.common.base.Key.SLICE_RECORD_COUNT;
import static com.wgzhao.addax.common.base.Key.TYPE;
import static com.wgzhao.addax.common.base.Key.VALUE;
import static com.wgzhao.addax.plugin.reader.datareader.DataKey.RULE;

public class DataReader
        extends Reader
{

    private enum Type
    {
        STRING, LONG, BOOL, DOUBLE, DATE, BYTES,
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
        private static final List<String> validUnits = Arrays.asList("d", "day", "M", "month", "y", "year", "h", "hour", "m", "minute",
                "s", "second", "w", "week");

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            // warn: 忽略大小写
            dealColumn(this.originalConfig);

            Long sliceRecordCount = this.originalConfig.getLong(SLICE_RECORD_COUNT);
            if (null == sliceRecordCount) {
                throw AddaxException.asAddaxException(DataReaderErrorCode.REQUIRED_VALUE, "sliceRecordCount is required");
            }
            else if (sliceRecordCount < 1) {
                throw AddaxException.asAddaxException(DataReaderErrorCode.ILLEGAL_VALUE, "sliceRecordCount must be greater than 1");
            }
        }

        private void dealColumn(Configuration originalConfig)
        {
            List<JSONObject> columns = originalConfig.getList(COLUMN, JSONObject.class);
            if (null == columns || columns.isEmpty()) {
                throw AddaxException.asAddaxException(DataReaderErrorCode.REQUIRED_VALUE, "column is required and NOT be empty");
            }

            List<String> dealColumns = new ArrayList<>();
            for (JSONObject eachColumn : columns) {
                Configuration eachColumnConfig = Configuration.from(eachColumn);
                try {
                    this.parseMixupFunctions(eachColumnConfig);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(DataReaderErrorCode.NOT_SUPPORT_TYPE,
                            String.format("Failed to parse column: %s", e.getMessage()), e);
                }
                String typeName = eachColumnConfig.getString(TYPE);
                if (StringUtils.isBlank(typeName)) {
                    // empty typeName will be set to default type: string
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
                                DataReaderErrorCode.NOT_SUPPORT_TYPE,
                                String.format("不支持类型[%s]", typeName));
                    }
                }
                dealColumns.add(eachColumnConfig.toJSON());
            }

            originalConfig.set(COLUMN, dealColumns);
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
         * <p>
         * date from : 指定开始日期，必填
         * interval: 隔间周期，默认为1，负数则递减 选填
         * unit: 间隔单位，默认为day，可设置为 d/day, m/month, y/year
         *
         * @param eachColumnConfig see {@link Configuration}
         */
        private void parseMixupFunctions(Configuration eachColumnConfig)
        {
            String columnRule = eachColumnConfig.getString(RULE, "constant").toLowerCase();
            if ("incr".equals(columnRule)) {
                validateIncrRule(eachColumnConfig);
            }
            else if ("random".equals(columnRule)) {
                validateRandom(eachColumnConfig);
            }
        }

        /**
         * 支持递增函数，当前仅支持整数类型，demo如下
         * LONG: incr 100 从100开始，每次加1
         * LONG: incr 0, 1 从0开始，每次加1
         * LONG: incr 1, 10 从 1 开始，每次加10
         * LONG: incr 1000, 1 从 1000开始，每次加-1，允许出现负数
         * DATE: incr &lt;date from&gt; &lt;interval&gt; &lt;unit&gt;
         *
         * @param eachColumnConfig {@link Configuration}
         */
        private void validateIncrRule(Configuration eachColumnConfig)
        {
            String value = eachColumnConfig.getString(VALUE);
            String dType = eachColumnConfig.getString(TYPE).toLowerCase();
            if ("long".equals(dType)) {
                //  columnValue is valid number ?
                if (!value.contains(",")) {
                    // setup the default step value
                    value = value + ",1";
                    eachColumnConfig.set(VALUE, value);
                }
                // validate value
                try {
                    Long.parseLong(value.split(",")[0].trim());
                    Long.parseLong(value.split(",")[1].trim());
                }
                catch (NumberFormatException e) {
                    throw AddaxException.asAddaxException(
                            DataReaderErrorCode.ILLEGAL_VALUE,
                            value + " is illegal, it must be a digital string"
                    );
                }
            }
            else if ("date".equals(dType)) {
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
                                DataReaderErrorCode.ILLEGAL_VALUE,
                                "The second field must be numeric, value [" + fields[1] + "] is not valid"
                        );
                    }
                    eachColumnConfig.set(VALUE, fields[0].trim() + "," + fields[1].trim() + ",d");
                }
                else {
                    String unit = fields[2].charAt(0) + "";
                    // validate unit
                    validateDateIncrUnit(unit);
                    // normalize unit to 1-char
                    eachColumnConfig.set(VALUE, fields[0].trim() + "," + fields[1].trim() + "," + unit);
                }
            }
            else {
                throw AddaxException.asAddaxException(
                        DataReaderErrorCode.NOT_SUPPORT_TYPE,
                        "递增序列当前仅支持整数类型(long)和日期类型(date)"
                );
            }
        }

        private void validateRandom(Configuration config)
        {
            String value = config.getString(VALUE);
            String[] split = value.split(",");
            if (split.length < 2) {
                throw AddaxException.asAddaxException(DataReaderErrorCode.ILLEGAL_VALUE,
                        String.format("Illegal random value [%s], supported random value like 'minVal, MaxVal[,scale]'", value));
            }
            String param1 = split[0];
            long param1Int;
            String param2 = split[1];
            long param2Int;
            if (StringUtils.isBlank(param1) && StringUtils.isBlank(param2)) {
                throw AddaxException.asAddaxException(
                        DataReaderErrorCode.ILLEGAL_VALUE,
                        String.format("random混淆函数不合法[%s], 混淆函数random的参数不能为空:%s, %s",
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
                            DataReaderErrorCode.ILLEGAL_VALUE,
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
                        DataReaderErrorCode.ILLEGAL_VALUE,
                        String.format("random 函数不合法[%s], 混淆函数random的参数不能为负数:%s, %s",
                                value, param1, param2));
            }
            if (!Type.BOOL.name().equalsIgnoreCase(typeName) && param1Int > param2Int) {
                throw AddaxException.asAddaxException(
                        DataReaderErrorCode.ILLEGAL_VALUE,
                        String.format("random 函数不合法[%s], 混淆函数random的参数需要第一个小于等于第二个:%s, %s",
                                value, param1, param2));
            }
            config.set(DataKey.MIXUP_FUNCTION_PARAM1, param1Int);
            config.set(DataKey.MIXUP_FUNCTION_PARAM2, param2Int);
            if (split.length == 3) {
                int scale = Integer.parseInt(split[2].trim());
                config.set(DataKey.MIXUP_FUNCTION_SCALE, scale);
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
                        DataReaderErrorCode.ILLEGAL_VALUE,
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
            if (columnRule == Rule.CONSTANT) {
                if ("null".equals(columnValue)) {
                    return null;
                }
                switch (columnType) {
                    case STRING:
                        return new StringColumn(columnValue);
                    case LONG:
                        return new LongColumn(columnValue);
                    case DOUBLE:
                        return new DoubleColumn(columnValue);
                    case DATE:
                        SimpleDateFormat format = new SimpleDateFormat(eachColumnConfig.getString(DATE_FORMAT, DEFAULT_DATE_FORMAT));
                        return new DateColumn(format.parse(columnValue));
                    case BOOL:
                        return new BoolColumn("true".equalsIgnoreCase(columnValue));
                    case BYTES:
                        return new BytesColumn(columnValue.getBytes());
                    default:
                        // in fact,never to be here
                        throw new Exception(columnType.name() + "is unsupported");
                }
            }
            if (columnRule == Rule.RANDOM) {
                if ("null".equals(columnValue)) {
                    return null;
                }
                long param1Int = eachColumnConfig.getLong(DataKey.MIXUP_FUNCTION_PARAM1, 0L);
                long param2Int = eachColumnConfig.getLong(DataKey.MIXUP_FUNCTION_PARAM2, 1L);
                int scale = eachColumnConfig.getInt(DataKey.MIXUP_FUNCTION_SCALE, -1);
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
                        }
                        else {
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
                    default:
                        // in fact,never to be here
                        throw new Exception(String.format("不支持类型[%s]", columnType.name()));
                }
            }

            if (columnRule == Rule.INCR) {
                Object currVal;
                long step;
                if (columnType == Type.LONG) {
                    //get initial value and step
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
                                    DataReaderErrorCode.ILLEGAL_VALUE,
                                    String.format("can not parse date value [%s] with date format [%s]", fields[0], datePattern)
                            );
                        }
                    }
                    incrMap.put(columnIndex, dateIncrement((Date) currVal, Integer.parseInt(fields[1]), fields[2]));
                    return new DateColumn((Date) currVal);
                }
                else {
                    throw AddaxException.asAddaxException(
                            DataReaderErrorCode.NOT_SUPPORT_TYPE,
                            columnType + " can not support for increment"
                    );
                }
            }

            // other rules
            switch (columnRule) {
                case ADDRESS:
                    return new StringColumn(AddressUtil.nextAddress());
                case BANK:
                    return new StringColumn(BankUtil.nextBank());
                case DEBIT_CARD:
                    return new StringColumn(BankUtil.nextDebitCard());
                case CREDIT_CARD:
                    return new StringColumn(BankUtil.nextCreditCard());
                case COMPANY:
                    return new StringColumn(CompanyUtil.nextCompany());
                case ID_CARD:
                    return new StringColumn(IdCardUtil.nextIdCard());
                case LAT:
                case LATITUDE:
                    return new DoubleColumn(GeoUtil.latitude().doubleValue());
                case LNG:
                case LONGITUDE:
                    return new DoubleColumn(GeoUtil.longitude().doubleValue());
                case JOB:
                    return new StringColumn(JobUtil.nextJob());
                case PHONE:
                    return new StringColumn(PhoneUtil.nextPhoneNumber());
                case STOCK_CODE:
                    return new StringColumn(StockUtil.nextStockCode());
                case STOCK_ACCOUNT:
                    return new StringColumn(StockUtil.nextStockAccount());
                case NAME:
                    return new StringColumn(PersonUtil.nextName());
                case UUID:
                    return new StringColumn(UUID.randomUUID().toString());
                default:
                    throw AddaxException.asAddaxException(DataReaderErrorCode.ILLEGAL_VALUE,
                            columnRule + " is unsupported");
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
                    throw AddaxException.asAddaxException(DataReaderErrorCode.ILLEGAL_VALUE,
                            "The date interval unit '" + unit + "' is unsupported");
            }
        }

        private Record buildOneRecord(RecordSender recordSender,
                List<String> columns)
        {
            if (null == recordSender) {
                throw new IllegalArgumentException("参数[recordSender]不能为空.");
            }

            if (null == columns || columns.isEmpty()) {
                throw new IllegalArgumentException("参数[column]不能为空.");
            }

            Record record = recordSender.createRecord();
            try {
                for (int i = 0; i < columns.size(); i++) {
                    Configuration eachColumnConfig = Configuration.from(columns.get(i));
                    record.addColumn(this.buildOneColumn(eachColumnConfig, i));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(DataReaderErrorCode.ILLEGAL_VALUE, "构造一个record失败.", e);
            }
            return record;
        }
    }
}
