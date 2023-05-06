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

package com.wgzhao.addax.common.statistics;

import com.wgzhao.addax.common.util.HostUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;

/**
 * Created by liqiang on 15/8/23.
 */
@SuppressWarnings("NullableProblems")
public class PerfRecord
        implements Comparable<PerfRecord>
{
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private final int taskGroupId;
    private final int taskId;
    private final PHASE phase;
    private volatile Date startTime; //NOSONAR
    private final long elapsedTimeInNs = -1;
    private volatile long count = 0;
    private volatile long size = 0;

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
        this.count += count;
    }

    public void addSize(long size)
    {
        this.size += size;
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
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s"
                , getInstId(), taskGroupId, taskId, phase,
                DateFormatUtils.format(startTime, DATETIME_FORMAT), elapsedTimeInNs, count, size, getHostIP());
    }

    @Override
    public int compareTo(PerfRecord o)
    {
        if (o == null) {
            return 1;
        }
        return Long.compare(this.elapsedTimeInNs, o.elapsedTimeInNs);
    }

    @Override
    public int hashCode()
    {
        long jobId = getInstId();
        int result = (int) (jobId ^ (jobId >>> 32));
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
        return count;
    }

    public long getSize()
    {
        return size;
    }

    public long getInstId()
    {
        return 0;
    }

    public String getHostIP()
    {
        return HostUtils.IP;
    }


    public enum PHASE
    {
        /**
         * task total运行的时间，前10为框架统计，后面为部分插件的个性统计
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
         * SQL_QUERY: sql query阶段, 部分reader的个性统计
         */
        SQL_QUERY(100),
        /**
         * 数据从sql全部读出来
         */
        RESULT_NEXT_ALL(101),

        /**
         * only odps block close
         */
        ODPS_BLOCK_CLOSE(102),

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
