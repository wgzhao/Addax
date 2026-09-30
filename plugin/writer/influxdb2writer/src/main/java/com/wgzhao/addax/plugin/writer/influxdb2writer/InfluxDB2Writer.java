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

package com.wgzhao.addax.plugin.writer.influxdb2writer;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.core.base.Key.BATCH_SIZE;
import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.ENDPOINT;
import static com.wgzhao.addax.core.base.Key.TABLE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Influx DB2 Writer. */
public class InfluxDB2Writer
        extends Writer
{
    /** The precisions the line protocol of InfluxDB 2.x understands. */
    private static final Set<String> PRECISIONS = Set.of("s", "ms", "us", "ns");

    private static final String DEFAULT_PRECISION = "ms";

    /** Job. */
    public static class Job
            extends Writer.Job
    {
        private Configuration originalConfig = null;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare()
        {
            Configuration connConf = originalConfig.getConfiguration(CONNECTION);
            if (connConf == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The required item 'connection' is not found");
            }
            connConf.getNecessaryValue(ENDPOINT, REQUIRED_VALUE);
            connConf.getNecessaryValue(InfluxDB2Key.BUCKET, REQUIRED_VALUE);
            connConf.getNecessaryValue(InfluxDB2Key.ORG, REQUIRED_VALUE);
            connConf.getNecessaryValue(TABLE, REQUIRED_VALUE);
            originalConfig.getNecessaryValue(InfluxDB2Key.TOKEN, REQUIRED_VALUE);

            List<String> columns = readStringList(originalConfig, COLUMN, "column");
            if (columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The column must be configured and '*' is not supported yet");
            }
            if (columns.contains("*")) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The column must be listed explicitly and '*' is not supported yet, but got " + columns);
            }
            Set<String> unique = new LinkedHashSet<>(columns);
            if (unique.size() != columns.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The column names must be unique, but got " + columns);
            }

            // the time column is prepended to the configured columns, so the tag columns have to be
            // part of the same list to keep the mapping to the record positions unambiguous
            List<String> tagColumns = readStringList(originalConfig, InfluxDB2Key.TAG_COLUMNS, "tagColumns");
            List<String> unknown = tagColumns.stream().filter(column -> !unique.contains(column)).toList();
            if (!unknown.isEmpty()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The tag column(s) %s are not listed in 'column', available column(s): %s"
                                .formatted(unknown, columns));
            }

            // both are read again by every task, validate them here so a broken value fails
            // before a single task starts
            readTags(originalConfig);
            parsePrecision(originalConfig.getString(InfluxDB2Key.INTERVAL, DEFAULT_PRECISION));

            int batchSize = originalConfig.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
            if (batchSize <= 0) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "'batchSize' must be a positive integer, but got " + batchSize);
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            // every task owns the records of exactly one reader slice, so the writer can run as
            // many tasks as the reader has slices
            return IntStream.range(0, adviceNumber).mapToObj(i -> originalConfig.clone()).toList();
        }

        @Override
        public void destroy()
        {
            //
        }
    }

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String endpoint;
        private String token;
        private String org;
        private String bucket;
        private String table;

        private List<String> columns;
        private Map<String, String> tags;
        private Set<String> tagColumns;
        private WritePrecision precision;
        private int batchSize;

        @Override
        public void init()
        {
            Configuration writerSliceConfig = super.getPluginJobConf();
            Configuration connConf = writerSliceConfig.getConfiguration(CONNECTION);
            this.endpoint = connConf.getString(ENDPOINT);
            this.org = connConf.getString(InfluxDB2Key.ORG);
            this.bucket = connConf.getString(InfluxDB2Key.BUCKET);
            this.table = connConf.getString(TABLE);

            this.token = writerSliceConfig.getString(InfluxDB2Key.TOKEN);
            this.columns = readStringList(writerSliceConfig, COLUMN, "column");
            this.tags = readTags(writerSliceConfig);
            this.tagColumns = new LinkedHashSet<>(
                    readStringList(writerSliceConfig, InfluxDB2Key.TAG_COLUMNS, "tagColumns"));
            this.precision = parsePrecision(writerSliceConfig.getString(InfluxDB2Key.INTERVAL, DEFAULT_PRECISION));
            this.batchSize = writerSliceConfig.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            try (InfluxDBClient influxDBClient = createClient()) {
                // the async WriteApi only publishes a failure as an event and lets the job finish
                // with a success, so the blocking one is used to keep the failure on the task thread
                WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
                List<Point> points = new ArrayList<>(batchSize);
                long skipped = 0;
                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    Point point = toPoint(record);
                    if (point == null) {
                        skipped++;
                        continue;
                    }
                    points.add(point);
                    if (points.size() >= batchSize) {
                        write(writeApi, points);
                    }
                }
                if (!points.isEmpty()) {
                    write(writeApi, points);
                }
                if (skipped > 0) {
                    LOG.warn("{} record(s) hold no field with a value and were not written, "
                            + "InfluxDB cannot store a point without a field", skipped);
                }
            }
        }

        @Override
        public void destroy()
        {
            //
        }

        /**
         * InfluxDB has no null, and the reader is free to hand over fewer values than the record
         * holds, so a missing value has to be dropped instead of being written as an empty one.
         */
        private Point toPoint(Record record)
        {
            int expected = columns.size() + 1;
            if (record.getColumnNumber() != expected) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        ("The record holds %d column(s) but the writer expects %d: the first column is the "
                                + "timestamp and 'column' lists %d name(s) %s")
                                .formatted(record.getColumnNumber(), expected, columns.size(), columns));
            }

            Point point = Point.measurement(table);
            // the static tags first so that a tag taken from a column wins on a name clash
            tags.forEach(point::addTag);
            point.time(toInstant(record.getColumn(0), record), precision);

            boolean hasField = false;
            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i);
                Object value = toValue(record.getColumn(i + 1));
                if (value == null) {
                    continue;
                }
                if (tagColumns.contains(name)) {
                    point.addTag(name, String.valueOf(value));
                }
                else {
                    addField(point, name, value);
                    hasField = true;
                }
            }
            if (!hasField) {
                LOG.debug("The record {} holds no field with a value, skipping it", record);
                return null;
            }
            return point;
        }

        /**
         * The value type is only known for sure once the record arrives, so the conversion follows
         * the column that carries it instead of a type collected up front.
         *
         * @return the value to write, or {@code null} when the column has to be skipped
         */
        private static Object toValue(Column column)
        {
            if (column == null || column.getRawData() == null) {
                return null;
            }
            return switch (column.getType()) {
                case LONG -> column.asLong();
                case DOUBLE -> {
                    Double value = column.asDouble();
                    // a non-finite double has no line protocol representation and would silently
                    // take the whole point down with it
                    yield (value == null || !Double.isFinite(value)) ? null : value;
                }
                case BOOL -> column.asBoolean();
                case DATE, TIMESTAMP -> column.asTimestamp().toInstant().toString();
                default -> column.asString();
            };
        }

        private static void addField(Point point, String name, Object value)
        {
            if (value instanceof Long || value instanceof Integer) {
                point.addField(name, ((Number) value).longValue());
            }
            else if (value instanceof Number number) {
                point.addField(name, number.doubleValue());
            }
            else if (value instanceof Boolean bool) {
                point.addField(name, bool);
            }
            else {
                point.addField(name, String.valueOf(value));
            }
        }

        private Instant toInstant(Column column, Record record)
        {
            if (column == null || column.getRawData() == null) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "The first column of the record must hold the timestamp, but it is empty: " + record);
            }
            return switch (column.getType()) {
                case TIMESTAMP, DATE -> column.asTimestamp().toInstant();
                // a numeric time column follows the Column#asTimestamp contract of this framework,
                // which reads it as epoch milliseconds
                case LONG, DOUBLE -> Instant.ofEpochMilli(column.asLong());
                case STRING -> parseInstant(column.asString());
                default -> throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "The first column of the record must hold the timestamp, but it is a %s: %s"
                                .formatted(column.getType(), record));
            };
        }

        /**
         * A text timestamp comes from a text file far more often than from a database, so the
         * accepted forms cover what such a file holds instead of only what the JDBC based readers
         * produce. The database form is parsed in the JVM time zone, which is what the readers of
         * this framework do with a string timestamp as well.
         */
        private static Instant parseInstant(String value)
        {
            try {
                return Timestamp.valueOf(value).toInstant();
            }
            catch (IllegalArgumentException ignored) {
                // not the "yyyy-MM-dd HH:mm:ss[.f]" form, try the others
            }
            try {
                return Instant.parse(value);
            }
            catch (DateTimeParseException ignored) {
                // not an ISO-8601 instant, try the other ISO-8601 shapes
            }
            try {
                return OffsetDateTime.parse(value).toInstant();
            }
            catch (DateTimeParseException ignored) {
                // no offset, try a local date time
            }
            try {
                return LocalDateTime.parse(value).atZone(ZoneId.systemDefault()).toInstant();
            }
            catch (DateTimeParseException ignored) {
                // not a date time at all, the last chance is epoch milliseconds
            }
            try {
                return Instant.ofEpochMilli(Long.parseLong(value));
            }
            catch (NumberFormatException e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        ("The timestamp [%s] of the first column cannot be parsed, the accepted forms are "
                                + "'yyyy-MM-dd HH:mm:ss[.SSS]', an ISO-8601 date time and epoch milliseconds")
                                .formatted(value));
            }
        }

        private void write(WriteApiBlocking writeApi, List<Point> points)
        {
            try {
                writeApi.writePoints(bucket, org, points);
            }
            catch (InfluxException e) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL,
                        "Failed to write %d point(s) into the bucket [%s], the job is stopped to avoid a silent data loss"
                                .formatted(points.size(), bucket), e);
            }
            points.clear();
        }

        private InfluxDBClient createClient()
        {
            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(endpoint)
                    .org(org)
                    .bucket(bucket)
                    .authenticateToken(token.toCharArray())
                    .build();
            // the line protocol compresses by a wide margin and the client sends it as it is
            // unless gzip is asked for
            return InfluxDBClientFactory.create(options).enableGzip();
        }
    }

    private static WritePrecision parsePrecision(String interval)
    {
        if (interval == null || !PRECISIONS.contains(interval.toLowerCase(Locale.ROOT))) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "'interval' must be one of %s, but got [%s]".formatted(PRECISIONS, interval));
        }
        return WritePrecision.valueOf(interval.toUpperCase(Locale.ROOT));
    }

    /**
     * The tag values are handed over to the client as strings: a numeric or boolean tag is a
     * perfectly normal line protocol tag, but the configuration keeps it as a JSON number.
     */
    private static Map<String, String> readTags(Configuration configuration)
    {
        Object raw = configuration.get(InfluxDB2Key.TAG);
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Collection<?> items)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "'tag' must be a list of {name: value} objects, but got " + raw);
        }
        Map<String, String> tags = new LinkedHashMap<>();
        for (Object item : items) {
            if (!(item instanceof Map<?, ?> tag)) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Every item of 'tag' must be a {name: value} object, but got " + item);
            }
            for (Map.Entry<?, ?> entry : tag.entrySet()) {
                String name = String.valueOf(entry.getKey());
                Object value = entry.getValue();
                if (name.isBlank()) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "The tag name must not be empty, but got " + tag);
                }
                if (value == null || value instanceof Map || value instanceof Collection) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            ("The tag [%s] must hold a single value, a tag of a nested object or a null "
                                    + "cannot be expressed by the line protocol").formatted(name));
                }
                tags.put(name, String.valueOf(value));
            }
        }
        return tags;
    }

    private static List<String> readStringList(Configuration configuration, String key, String what)
    {
        Object raw = configuration.get(key);
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof Collection<?> items)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "'%s' must be a list of strings, but got [%s]".formatted(what, raw));
        }
        List<String> values = new ArrayList<>(items.size());
        for (Object item : items) {
            if (!(item instanceof String value) || value.isBlank()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Every item of '%s' must be a non-blank string, but got [%s]".formatted(what, raw));
            }
            values.add(value);
        }
        return values;
    }
}
