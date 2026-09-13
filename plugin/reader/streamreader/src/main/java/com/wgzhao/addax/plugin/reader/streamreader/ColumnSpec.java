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

import com.wgzhao.addax.core.base.Constant;
import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.random.RandomGenerator;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/**
 * The generation rule of one column.
 * <p>
 * A column is parsed and validated exactly once - by {@link StreamReader.Job} to fail fast on a bad
 * configuration, and by {@link StreamReader.Task} to build the records - and the resulting rule is
 * applied to every record afterwards, so that no configuration item is read on the hot path.
 *
 * @param type the type of the column
 * @param rule how the value of the column is produced
 */
public record ColumnSpec(Type type, Rule rule)
{
    /** The supported column types. */
    public enum Type
    {
        STRING, LONG, BOOL, DOUBLE, DATE, BYTES, TIMESTAMP
    }

    /** How the value of a column is produced. */
    public sealed interface Rule
    {
    }

    /**
     * A fixed value, taken from the {@code value} item.
     *
     * @param value the raw value
     * @param dateMillis the parsed value of a {@link Type#DATE} column, 0 for the other types
     */
    public record ConstantValue(String value, long dateMillis) implements Rule
    {
    }

    /**
     * A random value taken from the inclusive range [{@code min}, {@code max}].
     * The bounds are lengths for a {@link Type#STRING}/{@link Type#BYTES} column and the ratio of
     * false to true for a {@link Type#BOOL} column.
     *
     * @param min the lower bound
     * @param max the upper bound
     * @param scale the number of decimal places of a {@link Type#DOUBLE} value, -1 if unset
     */
    public record Random(long min, long max, int scale) implements Rule
    {
    }

    /**
     * An arithmetic sequence of a {@link Type#LONG} column, the value is {@code start + n * step},
     * where {@code n} is the ordinal of the record inside the whole job.
     */
    public record Increment(long start, long step) implements Rule
    {
    }

    /**
     * A date sequence of a {@link Type#DATE} column, the value is
     * {@code base + n * step * unit}, where {@code n} is the ordinal of the record inside the whole job.
     *
     * @param baseMillis the start date, in epoch milliseconds
     */
    public record DateIncrement(long baseMillis, long step, ChronoUnit unit) implements Rule
    {
    }

    /**
     * A value produced by one of the {@link Generator built-in data rules}, the type of the column
     * is fixed by the rule.
     *
     * @param generator the rule that produces the value
     */
    public record Generated(Generator generator) implements Rule
    {
    }

    /** The value that stands for a SQL NULL. */
    private static final String NULL_VALUE = "null";

    /** The rules whose value type is selected by the type item, the other rules have a fixed type. */
    private static final String CONSTANT_RULE = "constant";
    private static final String RANDOM_RULE = "random";
    private static final String INCR_RULE = "incr";
    private static final Set<String> FUNCTION_RULES = Set.of(CONSTANT_RULE, RANDOM_RULE, INCR_RULE);
    private static final String FUNCTION_RULES_HINT = "constant, random, incr";

    /** The random range of a {@link Type#TIMESTAMP} column. */
    private static final long TIMESTAMP_LOWER_BOUND = 1_100_000_000_000L;
    private static final long TIMESTAMP_UPPER_BOUND = 2_100_000_000_000L;

    /** The interval units accepted by the increment function of a date column. */
    private static final String UNIT_HINT = "d/day, M/month, y/year, h/hour, m/minute, s/second, w/week";
    private static final Map<String, ChronoUnit> DATE_UNITS = Map.ofEntries(
            Map.entry("d", ChronoUnit.DAYS),
            Map.entry("day", ChronoUnit.DAYS),
            Map.entry("M", ChronoUnit.MONTHS),
            Map.entry("month", ChronoUnit.MONTHS),
            Map.entry("y", ChronoUnit.YEARS),
            Map.entry("year", ChronoUnit.YEARS),
            Map.entry("w", ChronoUnit.WEEKS),
            Map.entry("week", ChronoUnit.WEEKS),
            Map.entry("h", ChronoUnit.HOURS),
            Map.entry("hour", ChronoUnit.HOURS),
            Map.entry("m", ChronoUnit.MINUTES),
            Map.entry("minute", ChronoUnit.MINUTES),
            Map.entry("s", ChronoUnit.SECONDS),
            Map.entry("second", ChronoUnit.SECONDS));

    /**
     * Parse and validate the configuration of one column.
     * <p>
     * Two forms are supported. The {@code rule} item names the generation rule of the column and the
     * {@code value} item is its parameter; when {@code rule} is absent, the legacy form applies, where
     * the {@code random} and the {@code incr} items select the function of the column and a bare
     * {@code value} is a constant.
     *
     * @param column the configuration of the column
     * @return the generation rule of the column
     */
    public static ColumnSpec parse(Configuration column)
    {
        String rule = column.getString(StreamConstant.RULE);
        return StringUtils.isBlank(rule) ? parseFunctionColumn(column) : parseRuleColumn(column, rule);
    }

    /**
     * Parse a column that selects its rule with the {@code rule} item.
     *
     * @param column the configuration of the column
     * @param rule the configured rule name
     * @return the generation rule of the column
     */
    private static ColumnSpec parseRuleColumn(Configuration column, String rule)
    {
        Generator generator = Generator.of(rule);
        if (null != generator) {
            return parseGeneratedColumn(column, generator, rule);
        }

        String ruleName = Generator.normalize(rule);
        if (!FUNCTION_RULES.contains(ruleName)) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("The column rule [%s] is unsupported, the supported rules are %s, %s.",
                            rule, Generator.names(), FUNCTION_RULES_HINT));
        }

        Type type = parseType(column.getString(Key.TYPE));
        String dateFormat = dateFormat(column, type);
        String value = column.getNecessaryValue(Key.VALUE, REQUIRED_VALUE);
        return switch (ruleName) {
            case CONSTANT_RULE -> new ColumnSpec(type, constantValue(type, dateFormat, value));
            case RANDOM_RULE -> new ColumnSpec(type, parseRandom(type, dateFormat, value));
            default -> new ColumnSpec(type, parseIncrement(type, dateFormat, value));
        };
    }

    /**
     * Parse a column whose value comes from a built-in data rule. The type of such a column is fixed
     * by the rule, so a configured type is only accepted when it is the type of the rule.
     *
     * @param column the configuration of the column
     * @param generator the rule of the column
     * @param rule the configured rule name
     * @return the generation rule of the column
     */
    private static ColumnSpec parseGeneratedColumn(Configuration column, Generator generator, String rule)
    {
        String typeName = column.getString(Key.TYPE);
        if (StringUtils.isNotBlank(typeName) && generator.type() != parseType(typeName)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The rule [%s] always produces a %s value, the configured type [%s] is not supported.",
                            rule, generator.type(), typeName));
        }
        return new ColumnSpec(generator.type(), new Generated(generator));
    }

    /**
     * Parse a column in the legacy form, where the {@code random} and the {@code incr} items select
     * the function of the column.
     *
     * @param column the configuration of the column
     * @return the generation rule of the column
     */
    private static ColumnSpec parseFunctionColumn(Configuration column)
    {
        Type type = parseType(column.getString(Key.TYPE));
        String dateFormat = dateFormat(column, type);

        String random = column.getString(StreamConstant.RANDOM);
        if (StringUtils.isNotBlank(random)) {
            return new ColumnSpec(type, parseRandom(type, dateFormat, random));
        }

        String incr = column.getString(StreamConstant.INCR);
        if (StringUtils.isNotBlank(incr)) {
            return new ColumnSpec(type, parseIncrement(type, dateFormat, incr));
        }

        String value = column.getNecessaryValue(Key.VALUE, REQUIRED_VALUE);
        return new ColumnSpec(type, constantValue(type, dateFormat, value));
    }

    private static ConstantValue constantValue(Type type, String dateFormat, String value)
    {
        return new ConstantValue(value, Type.DATE == type ? parseDate(value, dateFormat).getTime() : 0L);
    }

    private static String dateFormat(Configuration column, Type type)
    {
        return Type.DATE == type ? column.getString(Key.DATE_FORMAT, Constant.DEFAULT_DATE_FORMAT) : null;
    }

    /**
     * Build the column of the record whose ordinal inside the whole job is {@code ordinal}.
     *
     * @param ordinal the ordinal of the record inside the whole job
     * @param rng the random generator of the task
     * @return the column of the record
     */
    public Column toColumn(long ordinal, RandomGenerator rng)
    {
        if (this.rule instanceof ConstantValue constant) {
            return constantColumn(constant);
        }
        if (this.rule instanceof Random random) {
            return randomColumn(random, rng);
        }
        if (this.rule instanceof Increment increment) {
            return new LongColumn(increment.start() + ordinal * increment.step());
        }
        if (this.rule instanceof DateIncrement increment) {
            Date date = Date.from(Instant.ofEpochMilli(increment.baseMillis())
                    .atZone(ZoneId.systemDefault())
                    .plus(ordinal * increment.step(), increment.unit())
                    .toInstant());
            return new DateColumn(date);
        }
        if (this.rule instanceof Generated generated) {
            return generated.generator().generate(rng);
        }
        throw new IllegalStateException("The rule " + this.rule + " is not supported.");
    }

    private Column constantColumn(ConstantValue constant)
    {
        if (NULL_VALUE.equals(constant.value())) {
            return new StringColumn();
        }
        return switch (this.type) {
            case STRING -> new StringColumn(constant.value());
            case LONG -> new LongColumn(constant.value());
            case DOUBLE -> new DoubleColumn(constant.value());
            case BOOL -> new BoolColumn(Boolean.parseBoolean(constant.value()));
            case DATE -> new DateColumn(new Date(constant.dateMillis()));
            case BYTES -> new BytesColumn(constant.value().getBytes(StandardCharsets.UTF_8));
            case TIMESTAMP -> new TimestampColumn(constant.value());
        };
    }

    private Column randomColumn(Random random, RandomGenerator rng)
    {
        return switch (this.type) {
            case STRING -> new StringColumn(RandomStringUtils.insecure().nextAlphanumeric(nextLength(random, rng)));
            case BYTES -> new BytesColumn(RandomStringUtils.insecure().nextAlphanumeric(nextLength(random, rng))
                    .getBytes(StandardCharsets.UTF_8));
            case LONG -> new LongColumn(rng.nextLong(random.min(), random.max() + 1));
            case DOUBLE -> new DoubleColumn(nextDouble(random, rng));
            case DATE -> new DateColumn(new Date(rng.nextLong(random.min(), random.max() + 1)));
            case BOOL -> new BoolColumn(nextBool(random, rng));
            // the configured bounds are meaningless for a timestamp, a reasonable range is used instead
            case TIMESTAMP -> new TimestampColumn(rng.nextLong(TIMESTAMP_LOWER_BOUND, TIMESTAMP_UPPER_BOUND));
        };
    }

    private static int nextLength(Random random, RandomGenerator rng)
    {
        return (int) rng.nextLong(random.min(), random.max() + 1);
    }

    private static double nextDouble(Random random, RandomGenerator rng)
    {
        double value = rng.nextDouble(random.min(), random.max() + 1);
        if (random.scale() > 0) {
            return BigDecimal.valueOf(value).setScale(random.scale(), RoundingMode.HALF_UP).doubleValue();
        }
        return value;
    }

    private static boolean nextBool(Random random, RandomGenerator rng)
    {
        // the bounds are the ratio of false to true, so `random 0, 10` never yields false
        if (0 == random.min()) {
            return true;
        }
        if (0 == random.max()) {
            return false;
        }
        // min + max outcomes, `max` of them are true
        return rng.nextLong(0, random.min() + random.max()) >= random.min();
    }

    private static Type parseType(String typeName)
    {
        if (StringUtils.isBlank(typeName)) {
            return Type.STRING;
        }
        try {
            return Type.valueOf(typeName.trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                    String.format("The column type [%s] is unsupported.", typeName));
        }
    }

    private static Rule parseRandom(Type type, String dateFormat, String random)
    {
        String[] fields = random.split(",");
        if (fields.length < 2 || fields.length > 3) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("Illegal random value [%s], the supported form is 'minVal, maxVal[, scale]'.", random));
        }

        long min;
        long max;
        if (Type.DATE == type) {
            min = parseDate(fields[0].trim(), dateFormat).getTime();
            max = parseDate(fields[1].trim(), dateFormat).getTime();
        }
        else {
            min = parseLong(fields[0], StreamConstant.RANDOM);
            max = parseLong(fields[1], StreamConstant.RANDOM);
            if (min < 0 || max < 0) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The random function's params [%s,%s] can not be negative.", fields[0], fields[1]));
            }
        }
        if (min > max && Type.BOOL != type) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The random function's params [%s,%s] is not valid, "
                            + "the first param must not be greater than the second one.", fields[0], fields[1]));
        }
        checkBounds(type, min, max);

        int scale = -1;
        if (3 == fields.length) {
            long parsedScale = parseLong(fields[2], "scale");
            if (parsedScale < 0 || parsedScale > Integer.MAX_VALUE) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The scale [%s] is out of range.", fields[2].trim()));
            }
            scale = (int) parsedScale;
        }
        return new Random(min, max, scale);
    }

    private static void checkBounds(Type type, long min, long max)
    {
        // max + 1 is used as the exclusive upper bound of every random range
        if (Long.MAX_VALUE == max) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    "The random function's second param is too large.");
        }
        // a length or a ratio is consumed as an int, a value range is not
        if (Type.LONG != type && Type.DOUBLE != type && Type.DATE != type
                && (min > Integer.MAX_VALUE || max > Integer.MAX_VALUE)) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The random function's params [%d,%d] are too large for a %s column.", min, max, type));
        }
    }

    private static Rule parseIncrement(Type type, String dateFormat, String incr)
    {
        String[] fields = incr.split(",");
        if (Type.LONG == type) {
            if (fields.length > 2) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Illegal incr value [%s], the supported form is 'startVal[, step]'.", incr));
            }
            long step = 2 == fields.length ? parseLong(fields[1], StreamConstant.INCR) : 1L;
            return new Increment(parseLong(fields[0], StreamConstant.INCR), step);
        }
        if (Type.DATE == type) {
            if (fields.length > 3) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("Illegal incr value [%s], the supported form is 'startDate[, step[, unit]]'.", incr));
            }
            long step = 2 <= fields.length && StringUtils.isNotBlank(fields[1])
                    ? parseLong(fields[1], StreamConstant.INCR)
                    : 1L;
            ChronoUnit unit = 3 == fields.length && StringUtils.isNotBlank(fields[2])
                    ? parseUnit(fields[2])
                    : ChronoUnit.DAYS;
            return new DateIncrement(parseDate(fields[0].trim(), dateFormat).getTime(), step, unit);
        }
        throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE,
                String.format("The %s type can not support the increment function.", type));
    }

    private static ChronoUnit parseUnit(String unit)
    {
        ChronoUnit chronoUnit = DATE_UNITS.get(unit.trim());
        if (null == chronoUnit) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The interval unit [%s] is not valid, the supported units are %s", unit.trim(), UNIT_HINT));
        }
        return chronoUnit;
    }

    private static long parseLong(String field, String item)
    {
        try {
            return Long.parseLong(field.trim());
        }
        catch (NumberFormatException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The %s item must be numeric, but got [%s].", item, field.trim()));
        }
    }

    private static Date parseDate(String value, String dateFormat)
    {
        try {
            TemporalAccessor parsed = DateTimeFormatter.ofPattern(dateFormat).parse(value);
            LocalTime time = parsed.query(TemporalQueries.localTime());
            if (null == time && parsed.isSupported(ChronoField.HOUR_OF_AMPM)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, String.format(
                        "The date value [%s] does not match the date format [%s]: 'hh' is the hour of a 12 hour "
                                + "clock and needs an AM/PM marker to be readable, use 'HH' for the hour of the day.",
                        value, dateFormat));
            }
            LocalDateTime dateTime = LocalDateTime.of(LocalDate.from(parsed), null == time ? LocalTime.MIDNIGHT : time);
            return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
        }
        catch (DateTimeException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The date value [%s] does not match the date format [%s].", value, dateFormat), e);
        }
    }
}
