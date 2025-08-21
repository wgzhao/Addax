/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.core.statistics;

import com.wgzhao.addax.core.util.HostUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_DATE_FORMAT;

/**
 * Created by liqiang on 15/8/23.
 */
public class PerfRecord
        implements Comparable<PerfRecord>
{
    private final int taskGroupId;
    private final int taskId;
    private final PHASE phase;
    private volatile Date startTime;
    private final AtomicLong count = new AtomicLong(0);
    private final AtomicLong size = new AtomicLong(0);

    public PerfRecord(int taskGroupId, int taskId, PHASE phase)
    {
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.phase = phase;
    }

    public void start()
    {
    }

    public void addCount(long count)
    {
        this.count.addAndGet(count);
    }

    public void addSize(long size)
    {
        this.size.addAndGet(size);
    }

    public void end()
    {
    }

    public void end(long elapsedTimeInNs)
    {
    }

    @Override
    public String toString()
    {
        long elapsedTimeInNs = -1;
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s"
                , getInstId(), taskGroupId, taskId, phase,
                DateFormatUtils.format(startTime, DEFAULT_DATE_FORMAT), elapsedTimeInNs, count, size, getHostIP());
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        result = 31 * result + taskGroupId;
        result = 31 * result + taskId;
        result = 31 * result + phase.toInt();
        result = 31 * result + (startTime != null ? startTime.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PerfRecord)) {
            return false;
        }

        PerfRecord dst = (PerfRecord) o;

        if (this.getInstId() != dst.getInstId()) {
            return false;
        }
        if (this.taskGroupId != dst.taskGroupId) {
            return false;
        }
        if (this.taskId != dst.taskId) {
            return false;
        }
        if (!Objects.equals(phase, dst.phase)) {
            return false;
        }
        return Objects.equals(startTime, dst.startTime);
    }

    public int getTaskGroupId()
    {
        return taskGroupId;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public PHASE getPhase()
    {
        return phase;
    }

    public long getCount()
    {
        return count.get();
    }

    public long getSize()
    {
        return size.get();
    }

    public long getInstId()
    {
        return 0;
    }

    public String getHostIP()
    {
        return HostUtils.IP;
    }

    @Override
    public int compareTo(PerfRecord o)
    {
        if (o == null) {
            return 1;
        }
        return 0;
    }

    public enum PHASE
    {
        /**
         * Total time of a task phase. The first 10 are framework phases, followed by plugin-specific ones.
         */
        TASK_TOTAL(0),

        READ_TASK_INIT(1),
        READ_TASK_PREPARE(2),
        READ_TASK_DATA(3),
        READ_TASK_POST(4),
        READ_TASK_DESTROY(5),

        WRITE_TASK_INIT(6),
        WRITE_TASK_PREPARE(7),
        WRITE_TASK_DATA(8),
        WRITE_TASK_POST(9),
        WRITE_TASK_DESTROY(10),

        /**
         * SQL_QUERY: SQL query phase, used by some readers as custom metrics.
         */
        SQL_QUERY(100),
        /**
         * All rows fetched from SQL result set.
         */
        RESULT_NEXT_ALL(101),

        WAIT_READ_TIME(103),

        WAIT_WRITE_TIME(104),

        TRANSFORMER_TIME(201);

        private final int val;

        PHASE(int val)
        {
            this.val = val;
        }

        public int toInt()
        {
            return val;
        }
    }
}
