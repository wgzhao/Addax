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

package com.wgzhao.addax.plugin.reader.influxdb2reader;

import com.influxdb.Cancellable;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.BytesColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.TimestampColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.wgzhao.addax.core.base.Key.COLUMN;
import static com.wgzhao.addax.core.base.Key.CONNECTION;
import static com.wgzhao.addax.core.base.Key.TABLE;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Influx DB2 Reader. */
public class InfluxDB2Reader
        extends Reader
{
    /**
     * The field/tag keys live in the InfluxDB index, so the schema functions answer without
     * scanning data. That keeps the column resolution independent from the time range.
     */
    private static final String SCHEMA_IMPORT = "import \"influxdata/influxdb/schema\"\n";

    private static final String TIME_COLUMN = "_time";

    private static final String MEASUREMENT_COLUMN = "_measurement";

    private static final String VALUE_COLUMN = "_value";

    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private Configuration originalConfig;

        @Override
        public void init()
        {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare()
        {
            this.originalConfig.getNecessaryValue(InfluxDB2Key.TOKEN, REQUIRED_VALUE);

            Configuration connConf = this.originalConfig.getConfiguration(CONNECTION);
            if (connConf == null) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The required item 'connection' is not found");
            }
            connConf.getNecessaryValue(InfluxDB2Key.ENDPOINT, REQUIRED_VALUE);
            connConf.getNecessaryValue(InfluxDB2Key.BUCKET, REQUIRED_VALUE);
            connConf.getNecessaryValue(InfluxDB2Key.ORG, REQUIRED_VALUE);

            // an unbounded flux query is rejected by the server, so a start time is mandatory
            List<String> range = this.originalConfig.getList(InfluxDB2Key.RANGE, String.class);
            if (range.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The required item 'range' is not found");
            }
            if (range.size() > 2 || range.stream().anyMatch(InfluxDB2Reader::isBlank)) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "'range' must hold one or two non-blank strings, either [start] or [start, stop], but got " + range);
            }

            Integer limit = this.originalConfig.getInt(InfluxDB2Key.LIMIT);
            if (limit != null && limit <= 0) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "'limit' must be a positive integer, but got " + limit);
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            return List.of(super.getPluginJobConf());
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

    /** Task. */
    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String endpoint;
        private String token;
        private String org;
        private String bucket;
        private List<String> measurements;
        private List<String> range;
        private List<String> columns;
        private Integer limit;

        @Override
        public void init()
        {
            Configuration readerSliceConfig = super.getPluginJobConf();
            Configuration connConf = readerSliceConfig.getConfiguration(CONNECTION);
            this.endpoint = connConf.getString(InfluxDB2Key.ENDPOINT);
            this.bucket = connConf.getString(InfluxDB2Key.BUCKET);
            this.org = connConf.getString(InfluxDB2Key.ORG);
            this.token = readerSliceConfig.getString(InfluxDB2Key.TOKEN);
            this.range = readerSliceConfig.getList(InfluxDB2Key.RANGE, String.class);
            this.limit = readerSliceConfig.getInt(InfluxDB2Key.LIMIT);
            this.measurements = new ArrayList<>(connConf.getList(TABLE, String.class));

            // an empty column list means "every column", same as a single '*'
            this.columns = readerSliceConfig.getList(COLUMN, String.class).stream()
                    .filter(column -> !"*".equals(column) && !isBlank(column))
                    .collect(Collectors.toCollection(ArrayList::new));
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            try (InfluxDBClient influxDBClient = createClient()) {
                Schema schema = resolveSchema(influxDBClient);
                if (schema.measurements().isEmpty()) {
                    LOG.warn("No measurement is available in bucket [{}], nothing to read", bucket);
                    return;
                }
                List<String> projection = resolveProjection(schema);
                String query = buildQuery(schema.measurements(), narrowDownFields(schema, projection));
                LOG.info("flux query: \n{}", query);
                read(influxDBClient, query, projection, recordSender);
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

        /**
         * InfluxDB answers the schema questions from its index, therefore the columns are known
         * before a single record is read and independent from the requested time range.
         */
        private Schema resolveSchema(InfluxDBClient influxDBClient)
        {
            List<String> target = this.measurements;
            if (target.isEmpty()) {
                target = distinctValues(influxDBClient, "schema.measurements(bucket: %s)".formatted(quote(bucket)));
                LOG.info("No measurement is configured, reading all measurements of bucket [{}]: {}", bucket, target);
            }

            Set<String> tags = new LinkedHashSet<>();
            Set<String> fields = new LinkedHashSet<>();
            List<String> known = new ArrayList<>();
            List<String> unknown = new ArrayList<>();
            for (String measurement : target) {
                List<String> fieldKeys = distinctValues(influxDBClient,
                        "schema.measurementFieldKeys(bucket: %s, measurement: %s, start: 0)"
                                .formatted(quote(bucket), quote(measurement)));
                // the internal keys (_start, _stop, _field, _measurement) are reported for every
                // measurement, missing ones included, so they cannot prove that it exists
                List<String> tagKeys = distinctValues(influxDBClient,
                        "schema.measurementTagKeys(bucket: %s, measurement: %s, start: 0)"
                                .formatted(quote(bucket), quote(measurement))).stream()
                        .filter(key -> !key.startsWith("_"))
                        .toList();
                // a measurement always holds at least one field, so an empty schema means "no such measurement"
                if (fieldKeys.isEmpty() && tagKeys.isEmpty()) {
                    unknown.add(measurement);
                    continue;
                }
                known.add(measurement);
                fields.addAll(fieldKeys);
                tags.addAll(tagKeys);
            }

            if (!unknown.isEmpty()) {
                String all = String.join(", ", distinctValues(influxDBClient,
                        "schema.measurements(bucket: %s)".formatted(quote(bucket))));
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The measurement(s) %s do not exist in bucket [%s], available measurement(s): [%s]"
                                .formatted(unknown, bucket, all));
            }
            return new Schema(List.copyOf(known), List.copyOf(tags), List.copyOf(fields));
        }

        /** Resolves the columns to send downstream, either every column or the requested ones. */
        private List<String> resolveProjection(Schema schema)
        {
            if (columns.isEmpty()) {
                List<String> all = new ArrayList<>();
                // without the measurement itself the rows of different measurements cannot be told apart
                if (schema.measurements().size() > 1) {
                    all.add(MEASUREMENT_COLUMN);
                }
                all.add(TIME_COLUMN);
                all.addAll(schema.tags());
                all.addAll(schema.fields());
                return all;
            }

            Set<String> available = new LinkedHashSet<>(List.of(TIME_COLUMN, MEASUREMENT_COLUMN, "_start", "_stop"));
            available.addAll(schema.tags());
            available.addAll(schema.fields());

            List<String> missing = columns.stream().filter(column -> !available.contains(column)).toList();
            if (!missing.isEmpty()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "The column(s) %s do not exist in measurement(s) %s, available column(s): %s"
                                .formatted(missing, schema.measurements(), available));
            }
            return List.copyOf(new LinkedHashSet<>(columns));
        }

        /**
         * The only chance to push the requested columns down to the server. Names that are not
         * fields ({@code _time}, tags, ...) must stay out of the predicate, otherwise the query
         * matches nothing.
         */
        private List<String> narrowDownFields(Schema schema, List<String> projection)
        {
            if (columns.isEmpty()) {
                return List.of();
            }
            return projection.stream().filter(schema.fields()::contains).toList();
        }

        private String buildQuery(List<String> targetMeasurements, List<String> requestedFields)
        {
            List<String> stages = new ArrayList<>();
            stages.add("from(bucket: %s)".formatted(quote(bucket)));
            List<String> rangeArgs = new ArrayList<>();
            rangeArgs.add("start: " + range.get(0));
            if (range.size() == 2) {
                rangeArgs.add("stop: " + range.get(1));
            }
            stages.add("  |> range(%s)".formatted(String.join(", ", rangeArgs)));
            if (!targetMeasurements.isEmpty()) {
                stages.add("  |> filter(fn: (r) => %s)".formatted(predicate("_measurement", targetMeasurements)));
            }
            if (!requestedFields.isEmpty()) {
                stages.add("  |> filter(fn: (r) => %s)".formatted(predicate("_field", requestedFields)));
            }
            // pivoting turns the fields into columns, which is what a record-based reader needs
            stages.add("  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")");
            if (limit != null) {
                stages.add("  |> limit(n: %d)".formatted(limit));
            }
            return String.join("\n", stages);
        }

        /**
         * Records are consumed as they arrive instead of materializing the whole result set,
         * the blocking {@link RecordSender} keeps the OkHttp thread in step with the writer.
         */
        private void read(InfluxDBClient influxDBClient, String query, List<String> projection, RecordSender recordSender)
        {
            String[] names = projection.toArray(new String[0]);
            CountDownLatch finished = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            BiConsumer<Cancellable, FluxRecord> onNext = (cancellable, fluxRecord) -> {
                try {
                    Record record = recordSender.createRecord();
                    for (String name : names) {
                        record.addColumn(toColumn(fluxRecord.getValueByKey(name)));
                    }
                    recordSender.sendToWriter(record);
                }
                catch (Throwable e) {
                    failure.compareAndSet(null, e);
                    cancellable.cancel();
                    finished.countDown();
                }
            };

            // the callbacks run on the OkHttp dispatcher, so a failure has to be handed back
            // to the task thread instead of being thrown here where nobody observes it
            influxDBClient.getQueryApi().query(query, onNext,
                    throwable -> {
                        failure.compareAndSet(null, throwable);
                        finished.countDown();
                    },
                    finished::countDown);

            try {
                finished.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw AddaxException.asAddaxException(EXECUTE_FAIL, "Interrupted while reading from InfluxDB", e);
            }

            Throwable throwable = failure.get();
            if (throwable instanceof AddaxException addaxException) {
                throw addaxException;
            }
            if (throwable != null) {
                throw AddaxException.asAddaxException(EXECUTE_FAIL, throwable);
            }
        }

        private InfluxDBClient createClient()
        {
            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(endpoint)
                    .authenticateToken(token.toCharArray())
                    .org(org)
                    .build();
            // the client asks for an identity encoded body unless gzip is turned on, but the
            // annotated csv of a large read compresses by a wide margin
            return InfluxDBClientFactory.create(options).enableGzip();
        }
    }

    /** The columns available in the requested measurements. */
    private record Schema(List<String> measurements, List<String> tags, List<String> fields)
    {
    }

    private static List<String> distinctValues(InfluxDBClient influxDBClient, String query)
    {
        List<String> values = new ArrayList<>();
        for (FluxTable table : influxDBClient.getQueryApi().query(SCHEMA_IMPORT + query)) {
            for (FluxRecord record : table.getRecords()) {
                Object value = record.getValueByKey(VALUE_COLUMN);
                if (value != null) {
                    values.add(value.toString());
                }
            }
        }
        return values;
    }

    /**
     * The value type is only known for sure once a record arrives: the same field name can be
     * a long in one measurement and a double in another, so the conversion follows the value
     * instead of a type collected up front.
     */
    private static Column toColumn(Object value)
    {
        if (value == null) {
            return new StringColumn();
        }
        if (value instanceof Boolean bool) {
            return new BoolColumn(bool);
        }
        if (value instanceof Long number) {
            return new LongColumn(number);
        }
        if (value instanceof Double number) {
            return new DoubleColumn(number);
        }
        if (value instanceof Instant instant) {
            return new TimestampColumn(Timestamp.from(instant));
        }
        if (value instanceof byte[] bytes) {
            return new BytesColumn(bytes);
        }
        return new StringColumn(value.toString());
    }

    private static String predicate(String column, Collection<String> values)
    {
        return values.stream()
                .map(value -> "r.%s == %s".formatted(column, quote(value)))
                .collect(Collectors.joining(" or "));
    }

    private static String quote(String value)
    {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static boolean isBlank(String value)
    {
        return value == null || value.isBlank();
    }
}
