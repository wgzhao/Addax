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

import com.wgzhao.addax.core.base.Key;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.spi.Reader;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.random.RandomGenerator;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

/** Stream Reader. */
public class StreamReader
        extends Reader
{
    /** Job. */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init()
        {
            this.originalConfig = getPluginJobConf();
            dealColumn(this.originalConfig);

            Long sliceRecordCount = this.originalConfig.getLong(Key.SLICE_RECORD_COUNT);
            if (null == sliceRecordCount) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The item sliceRecordCount is required.");
            }
            else if (sliceRecordCount < 1) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "The value of item sliceRecordCount must be greater than 0.");
            }
        }

        /**
         * Validate every column and normalize it into the form that the tasks consume.
         * The column is parsed here as well, so that an illegal configuration fails the job
         * before any task is started.
         *
         * @param originalConfig the configuration of the job
         */
        private void dealColumn(Configuration originalConfig)
        {
            List<Configuration> columns = originalConfig.getListConfiguration(Key.COLUMN);
            if (columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE,
                        "The item column is required.");
            }

            List<String> normalizedColumns = new ArrayList<>(columns.size());
            for (Configuration eachColumn : columns) {
                removeConflictedItems(eachColumn);
                ColumnSpec.parse(eachColumn);
                normalizedColumns.add(eachColumn.toJSON());
            }
            originalConfig.set(Key.COLUMN, normalizedColumns);
        }

        /**
         * A constant value is prior to the random and increment functions, drop the functions when
         * more than one of them is configured on the same column.
         *
         * @param column the configuration of the column
         */
        private void removeConflictedItems(Configuration column)
        {
            String columnValue = column.getString(Key.VALUE);
            String columnRandom = column.getString(StreamConstant.RANDOM);
            String columnIncr = column.getString(StreamConstant.INCR);
            if (StringUtils.isBlank(columnValue) || StringUtils.isAllBlank(columnRandom, columnIncr)) {
                return;
            }

            LOG.warn("The column value [{}] is a constant, the configured random [{}] / incr [{}] function is ignored.",
                    columnValue, columnRandom, columnIncr);
            if (StringUtils.isNotBlank(columnRandom)) {
                column.remove(StreamConstant.RANDOM);
            }
            if (StringUtils.isNotBlank(columnIncr)) {
                column.remove(StreamConstant.INCR);
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            List<Configuration> configurations = new ArrayList<>(adviceNumber);
            for (int i = 0; i < adviceNumber; i++) {
                Configuration configuration = this.originalConfig.clone();
                // The increment functions have to produce a unique sequence over all the slices.
                // Instead of sharing a counter between the concurrently running tasks, every slice
                // takes a disjoint part of the sequence: the record whose ordinal inside the whole
                // job is `n` belongs to the slice `n % adviceNumber` and is its `n / adviceNumber`
                // record. So a slice starts at `i` and steps by `adviceNumber`.
                configuration.set(StreamConstant.SLICE_INDEX, i);
                configuration.set(StreamConstant.SLICE_COUNT, adviceNumber);
                configurations.add(configuration);
            }
            return configurations;
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
        private List<ColumnSpec> columns;

        private long sliceRecordCount;

        /** The ordinal of the first record of this slice inside the whole job. */
        private int sliceIndex;

        /** The total number of slices of the job. */
        private int sliceCount;

        /** True when every column of the record is a constant, the record is built only once. */
        private boolean fixedValue;

        private final RandomGenerator rng = RandomGenerator.of("Xoroshiro128PlusPlus");

        @Override
        public void init()
        {
            Configuration readerSliceConfig = getPluginJobConf();
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class).stream()
                    .map(column -> ColumnSpec.parse(Configuration.from(column)))
                    .toList();

            this.sliceRecordCount = readerSliceConfig.getLong(Key.SLICE_RECORD_COUNT);
            this.sliceIndex = readerSliceConfig.getInt(StreamConstant.SLICE_INDEX, 0);
            this.sliceCount = readerSliceConfig.getInt(StreamConstant.SLICE_COUNT, 1);
            this.fixedValue = this.columns.stream().allMatch(column -> column.rule() instanceof ColumnSpec.ConstantValue);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            if (this.fixedValue) {
                Record record = buildOneRecord(recordSender, 0L);
                for (long i = 0; i < this.sliceRecordCount; i++) {
                    recordSender.sendToWriter(record);
                }
                return;
            }

            long ordinal = this.sliceIndex;
            for (long i = 0; i < this.sliceRecordCount; i++) {
                recordSender.sendToWriter(buildOneRecord(recordSender, ordinal));
                ordinal += this.sliceCount;
            }
        }

        @Override
        public void destroy()
        {
            //
        }

        /**
         * Build the record whose ordinal inside the whole job is {@code ordinal}.
         *
         * @param recordSender the sender of the record
         * @param ordinal the ordinal of the record inside the whole job
         * @return the record
         */
        private Record buildOneRecord(RecordSender recordSender, long ordinal)
        {
            Record record = recordSender.createRecord();
            try {
                for (ColumnSpec column : this.columns) {
                    record.addColumn(column.toColumn(ordinal, this.rng));
                }
            }
            catch (AddaxException e) {
                throw e;
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        "Failed to build record.", e);
            }
            return record;
        }
    }
}
