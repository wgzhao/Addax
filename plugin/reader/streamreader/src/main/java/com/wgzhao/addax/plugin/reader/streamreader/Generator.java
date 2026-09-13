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

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.plugin.reader.streamreader.ColumnSpec.Type;
import com.wgzhao.addax.plugin.reader.streamreader.util.AddressUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.BankUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.CompanyUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.EmailUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.GeoUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.IdCardUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.JobUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.PersonUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.PhoneUtil;
import com.wgzhao.addax.plugin.reader.streamreader.util.StockUtil;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The built-in data rules.
 * <p>
 * A rule produces values that look like the business data of a real system - an ID card number,
 * a bank card number, an address - as opposed to the typed random values of a
 * {@link ColumnSpec.Random}. The type of such a column is fixed by the rule itself.
 * <p>
 * The name of a rule is matched ignoring case, underscores and dashes, so {@code idCard},
 * {@code id_card} and {@code ID_CARD} are the same rule.
 */
public enum Generator
{
    /** A domestic address. */
    ADDRESS(Type.STRING, rng -> new StringColumn(AddressUtil.nextAddress(rng))),
    /** A domestic bank name. */
    BANK(Type.STRING, rng -> new StringColumn(BankUtil.nextBank(rng))),
    /** A domestic company name. */
    COMPANY(Type.STRING, rng -> new StringColumn(CompanyUtil.nextCompany(rng))),
    /** A 16 digits credit card number. */
    CREDIT_CARD(Type.STRING, rng -> new StringColumn(BankUtil.nextCreditCard(rng))),
    /** A 19 digits debit card number. */
    DEBIT_CARD(Type.STRING, rng -> new StringColumn(BankUtil.nextDebitCard(rng))),
    /** An email address. */
    EMAIL(Type.STRING, rng -> new StringColumn(EmailUtil.nextEmail(rng))),
    /** An 18 digits domestic ID card number with a valid checksum and area code. */
    ID_CARD(Type.STRING, rng -> new StringColumn(IdCardUtil.nextIdCard(rng))),
    /** A job title. */
    JOB(Type.STRING, rng -> new StringColumn(JobUtil.nextJob(rng))),
    /** A latitude in [-90, 90) with 7 decimal places, also known as {@code lat}. */
    LATITUDE(Type.DOUBLE, rng -> new DoubleColumn(GeoUtil.latitude(rng).doubleValue())),
    /** A longitude in [-180, 180) with 7 decimal places, also known as {@code lng}. */
    LONGITUDE(Type.DOUBLE, rng -> new DoubleColumn(GeoUtil.longitude(rng).doubleValue())),
    /** A domestic name. */
    NAME(Type.STRING, rng -> new StringColumn(PersonUtil.nextName(rng))),
    /** A domestic mobile phone number. */
    PHONE(Type.STRING, rng -> new StringColumn(PhoneUtil.nextPhoneNumber(rng))),
    /** A 10 digits stock trading account. */
    STOCK_ACCOUNT(Type.STRING, rng -> new StringColumn(StockUtil.nextStockAccount(rng))),
    /** A 6 digits stock symbol. */
    STOCK_CODE(Type.STRING, rng -> new StringColumn(StockUtil.nextStockCode(rng))),
    /** A random UUID. */
    UUID(Type.STRING, rng -> new StringColumn(java.util.UUID.randomUUID().toString())),
    /** A 6 digits postal code. */
    ZIP_CODE(Type.LONG, rng -> new LongColumn(rng.nextLong(Generator.ZIP_LOWER_BOUND, Generator.ZIP_UPPER_BOUND)));

    /**
     * The range of a postal code. Postal codes are 6 digits and the leading one is never zero,
     * a value below 100000 would lose its leading digit as a number.
     */
    private static final long ZIP_LOWER_BOUND = 100000;
    private static final long ZIP_UPPER_BOUND = 699000;

    private static final Map<String, Generator> BY_NAME = buildNames();

    private final Type type;

    private final Function<RandomGenerator, Column> generator;

    Generator(Type type, Function<RandomGenerator, Column> generator)
    {
        this.type = type;
        this.generator = generator;
    }

    /**
     * Look up a rule by its configured name.
     *
     * @param name the name of the rule
     * @return the rule, or {@code null} when no rule has that name
     */
    public static Generator of(String name)
    {
        return null == name ? null : BY_NAME.get(normalize(name));
    }

    /**
     * The names of all the rules, as they are written in the documentation, for an error message.
     *
     * @return the names separated by a comma
     */
    public static String names()
    {
        return Stream.of(values()).map(generator -> toCamelCase(generator.name()))
                .collect(Collectors.joining(", "));
    }

    /** Turn the name of a constant into the camel case spelling of the documentation. */
    private static String toCamelCase(String constant)
    {
        StringBuilder name = new StringBuilder(constant.length());
        boolean upper = false;
        for (char c : constant.toCharArray()) {
            if ('_' == c) {
                upper = true;
                continue;
            }
            name.append(upper ? c : Character.toLowerCase(c));
            upper = false;
        }
        return name.toString();
    }

    private static Map<String, Generator> buildNames()
    {
        Map<String, Generator> names = new HashMap<>();
        for (Generator generator : values()) {
            names.put(normalize(generator.name()), generator);
        }
        // the aliases promised by the documentation
        names.put("lat", LATITUDE);
        names.put("lng", LONGITUDE);
        return Map.copyOf(names);
    }

    /**
     * Normalize the configured name of a rule, so that its spelling does not matter.
     *
     * @param name the configured name
     * @return the name without case, underscores and dashes
     */
    static String normalize(String name)
    {
        return name.trim().toLowerCase(Locale.ROOT).replace("_", "").replace("-", "");
    }

    /** The type of the value produced by this rule. */
    public Type type()
    {
        return this.type;
    }

    /**
     * Build the value of this rule.
     *
     * @param rng the random generator of the task
     * @return the column
     */
    public Column generate(RandomGenerator rng)
    {
        return this.generator.apply(rng);
    }
}
