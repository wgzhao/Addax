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

package com.wgzhao.addax.plugin.writer.iotdbwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.plugin.TaskPluginCollector;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;

/** Writes records to the IoTDB tree model through the native Session API. */
final class IotdbTreeWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(IotdbTreeWriter.class);

    private final IotdbWriterConfig config;
    private TSDataType[] measurementTypes;

    IotdbTreeWriter(IotdbWriterConfig config)
    {
        this.config = config;
        if (!config.columnTypes().isEmpty()) {
            this.measurementTypes = config.columnTypes().toArray(new TSDataType[0]);
        }
    }

    void write(RecordReceiver receiver, TaskPluginCollector collector)
    {
        int expectedColumns = config.measurements().size() + 1;
        List<Record> buffer = new ArrayList<>(config.batchSize());
        long records = 0;
        try (Session session = openSession(config)) {
            Record record;
            while ((record = receiver.getFromReader()) != null) {
                if (record.getColumnNumber() != expectedColumns) {
                    collector.collectDirtyRecord(record,
                            "The record holds " + record.getColumnNumber() + " column(s), but the writer expects "
                                    + expectedColumns + " (time + " + config.measurements().size() + " measurement(s)).");
                    continue;
                }
                buffer.add(record);
                records++;
                if (buffer.size() >= config.batchSize()) {
                    flush(session, buffer, collector);
                }
            }
            if (!buffer.isEmpty()) {
                flush(session, buffer, collector);
            }
        }
        catch (AddaxException e) {
            throw e;
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "Failed to write records to IoTDB device " + config.device(), e);
        }
        LOG.info("Finished writing {} record(s) to IoTDB device {}.", records, config.device());
    }

    static Session openSession(IotdbWriterConfig config)
            throws Exception
    {
        Session.Builder builder = new Session.Builder()
                .host(config.host())
                .port(config.port())
                .username(config.username())
                .password(config.password());
        if (config.database() != null) {
            builder.database(config.database());
        }
        Session session = builder.build();
        session.open();
        return session;
    }

    private void flush(Session session, List<Record> buffer, TaskPluginCollector collector)
    {
        if (this.measurementTypes == null) {
            resolveTypes(buffer);
        }

        List<Record> validRecords = new ArrayList<>(buffer.size());
        List<Long> times = new ArrayList<>(buffer.size());
        List<List<Object>> valuesList = new ArrayList<>(buffer.size());
        for (Record record : buffer) {
            try {
                long time = IotdbWriterUtil.extractTimestamp(record, config.timeColumnPosition()).getTime();
                List<Object> values = new ArrayList<>(this.measurementTypes.length);
                for (int i = 0; i < this.measurementTypes.length; i++) {
                    int recordIndex = i < config.timeColumnPosition() ? i : i + 1;
                    values.add(toValue(record.getColumn(recordIndex), this.measurementTypes[i]));
                }
                times.add(time);
                valuesList.add(values);
                validRecords.add(record);
            }
            catch (Exception e) {
                collector.collectDirtyRecord(record, e);
            }
        }
        buffer.clear();
        if (times.isEmpty()) {
            return;
        }

        List<TSDataType> types = Arrays.asList(this.measurementTypes);
        List<List<String>> measurementsList = Collections.nCopies(times.size(), config.measurements());
        List<List<TSDataType>> typesList = Collections.nCopies(times.size(), types);
        try {
            session.insertRecordsOfOneDevice(config.device(), times, measurementsList, typesList, valuesList);
            LOG.debug("Wrote a batch of {} record(s) to {}.", times.size(), config.device());
        }
        catch (Exception batchError) {
            LOG.warn("The batch write to {} failed, retrying {} record(s) individually: {}",
                    config.device(), times.size(), batchError.getMessage());
            for (int i = 0; i < times.size(); i++) {
                try {
                    session.insertRecord(config.device(), times.get(i), config.measurements(), types, valuesList.get(i));
                }
                catch (Exception e) {
                    collector.collectDirtyRecord(validRecords.get(i), e);
                }
            }
        }
    }

    /**
     * A measurement whose values are all null in the first batch still needs a type for the
     * Session API. A configured columnTypes wins; otherwise the first non-null value decides,
     * and TEXT is the fallback so that an all-null column still creates a usable measurement.
     */
    private void resolveTypes(List<Record> records)
    {
        this.measurementTypes = new TSDataType[config.measurements().size()];
        for (int i = 0; i < this.measurementTypes.length; i++) {
            TSDataType type = null;
            for (Record record : records) {
                int recordIndex = i < config.timeColumnPosition() ? i : i + 1;
                Column column = record.getColumn(recordIndex);
                if (column != null && column.getRawData() != null) {
                    type = toDataType(column);
                    break;
                }
            }
            if (type == null) {
                LOG.warn("Measurement {} is null in the first batch and defaults to TEXT; "
                        + "set columnTypes if another type is required.", config.measurements().get(i));
                type = TSDataType.TEXT;
            }
            this.measurementTypes[i] = type;
        }
        LOG.info("Resolved IoTDB measurement types {} for device {}.",
                Arrays.toString(this.measurementTypes), config.device());
    }

    private static TSDataType toDataType(Column column)
    {
        return switch (column.getType()) {
            case BOOL -> TSDataType.BOOLEAN;
            case INT, LONG -> TSDataType.INT64;
            case DOUBLE -> TSDataType.DOUBLE;
            case STRING -> TSDataType.TEXT;
            case DATE, TIMESTAMP -> TSDataType.TIMESTAMP;
            case BYTES -> TSDataType.BLOB;
            default -> TSDataType.TEXT;
        };
    }

    private static Object toValue(Column column, TSDataType type)
    {
        if (column == null || column.getRawData() == null) {
            return null;
        }
        return switch (type) {
            case BOOLEAN -> column.asBoolean();
            case INT32 -> {
                Long value = column.asLong();
                if (value == null) {
                    yield null;
                }
                if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                    throw AddaxException.asAddaxException(EXECUTE_FAIL,
                            "Value " + value + " is out of range for IoTDB INT32.");
                }
                yield value.intValue();
            }
            case INT64 -> column.asLong();
            case FLOAT -> {
                Double value = column.asDouble();
                yield value == null ? null : value.floatValue();
            }
            case DOUBLE -> column.asDouble();
            case TEXT, STRING -> column.asString();
            case TIMESTAMP -> {
                java.sql.Timestamp timestamp = column.asTimestamp();
                yield timestamp == null ? null : timestamp.getTime();
            }
            case BLOB -> column.asBytes();
            default -> throw AddaxException.asAddaxException(EXECUTE_FAIL,
                    "Cannot convert a record column to IoTDB type " + type);
        };
    }
}
