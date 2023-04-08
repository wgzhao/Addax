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

import com.wgzhao.addax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * PerfTrace 记录 job（local模式），taskGroup（distribute模式），因为这2种都是jvm，即一个jvm里只需要有1个PerfTrace。
 */

public class PerfTrace
{

    private static final Logger LOG = LoggerFactory.getLogger(PerfTrace.class);
    private static PerfTrace instance;
    //jobid_jobversion, instanceid, taskid, src_mark, dst_mark,
    private final Map<Integer, String> taskDetails = new ConcurrentHashMap<>();
    //PHASE => PerfRecord
    private final ConcurrentHashMap<PerfRecord.PHASE, SumPerfRecord4Print> perfRecordMaps4print = new ConcurrentHashMap<>();
    // job_phase => SumPerf4Report
    private final SumPerf4Report sumPerf4Report = new SumPerf4Report();
    private final Set<PerfRecord> needReportPool4NotEnd = new HashSet<>();
    private final List<PerfRecord> totalEndReport = new ArrayList<>();
    private volatile boolean enable;
    private volatile boolean isJob;
    private long instId;
    private long jobId;
    private int batchSize = 500;
    private volatile boolean perfReportEnable = true;

    private PerfTrace(boolean isJob, long jobId, int taskGroupId, boolean enable)
    {
        try {
            String perfTraceId = isJob ? "job_" + jobId : String.format("taskGroup_%s_%s", jobId, taskGroupId);
            this.enable = enable;
            this.isJob = isJob;
            this.instId = jobId;
            LOG.info(String.format("PerfTrace traceId=%s, isEnable=%s", perfTraceId, this.enable));
        }
        catch (Exception e) {
            // do nothing
            this.enable = false;
        }
    }

    public static synchronized PerfTrace getInstance(boolean isJob, long jobId, int taskGroupId, boolean enable)
    {
        if (instance == null) {
            instance = new PerfTrace(isJob, jobId, taskGroupId, enable);
        }
        return instance;
    }

    /*
     * 因为一个JVM只有一个，因此在getInstance(isJob,jobId,taskGroupId)调用完成实例化后，方便后续调用，直接返回该实例
     */
    public static synchronized PerfTrace getInstance()
    {
        if (instance == null) {
            LOG.error("PerfTrace instance not be init! must have some error! ");
            instance = new PerfTrace(false, -1111, -1111, false);
        }
        return instance;
    }

    //缺省传入的时间是nano
    public static String unitTime(long time)
    {
        return unitTime(time, TimeUnit.NANOSECONDS);
    }

    public static String unitTime(long time, TimeUnit timeUnit)
    {
        return String.format("%,.3fs", ((float) timeUnit.toNanos(time)) / 1000000000);
    }

    public static String unitSize(long size)
    {
        if (size > 1000000000) {
            return String.format("%,.2fG", (float) size / 1000000000);
        }
        else if (size > 1000000) {
            return String.format("%,.2fM", (float) size / 1000000);
        }
        else if (size > 1000) {
            return String.format("%,.2fK", (float) size / 1000);
        }
        else {
            return size + "B";
        }
    }

    public void addTaskDetails(int taskId, String detail)
    {
        if (enable) {
            String before = "";
            int index = detail.indexOf("?");
            String current = detail.substring(0, index == -1 ? detail.length() : index);
            if (current.contains("[")) {
                current += "]";
            }
            if (taskDetails.containsKey(taskId)) {
                before = taskDetails.get(taskId).trim();
            }
            if (StringUtils.isEmpty(before)) {
                before = "";
            }
            else {
                before += ",";
            }
            this.taskDetails.put(taskId, before + current);
        }
    }

    public void tracePerfRecord(PerfRecord perfRecord)
    {
        try {
            if (enable) {
                long curNanoTime = System.nanoTime();
                //ArrayList非线程安全
                PerfRecord.ACTION action = perfRecord.getAction();
                if (action == PerfRecord.ACTION.END) {
                    synchronized (totalEndReport) {
                        totalEndReport.add(perfRecord);

                        if (totalEndReport.size() > batchSize * 10) {
                            sumPerf4EndPrint(totalEndReport);
                        }
                    }

                    if (perfReportEnable && needReport(perfRecord)) {
                        synchronized (needReportPool4NotEnd) {
                            sumPerf4Report.add(curNanoTime, perfRecord);
                            needReportPool4NotEnd.remove(perfRecord);
                        }
                    }
                }
                else if (action == PerfRecord.ACTION.START && perfReportEnable && needReport(perfRecord)) {
                    synchronized (needReportPool4NotEnd) {
                        needReportPool4NotEnd.add(perfRecord);
                    }
                }
            }
        }
        catch (Exception e) {
            // do nothing
        }
    }

    private boolean needReport(PerfRecord perfRecord)
    {
        switch (perfRecord.getPhase()) {
            case TASK_TOTAL:
            case SQL_QUERY:
            case RESULT_NEXT_ALL:
            case ODPS_BLOCK_CLOSE:
                return true;
            default:
                return false;
        }
    }

    public String summarizeNoException()
    {
        String res;
        try {
            res = summarize();
        }
        catch (Exception e) {
            res = "PerfTrace summarize has Exception " + e.getMessage();
        }
        return res;
    }

    //任务结束时，对当前的perf总汇总统计
    private synchronized String summarize()
    {
        if (!enable) {
            return "PerfTrace not enable!";
        }

        if (!totalEndReport.isEmpty()) {
            sumPerf4EndPrint(totalEndReport);
        }

        StringBuilder info = new StringBuilder();
        info.append("%n === total summarize info === %n");
        info.append("%n   1. all phase average time info and max time task info: %n%n");
        info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %-100s%n", "PHASE", "AVERAGE USED TIME", "ALL TASK NUM", "MAX USED TIME", "MAX TASK ID", "MAX TASK INFO"));

        List<PerfRecord.PHASE> keys = new ArrayList<>(perfRecordMaps4print.keySet());
        keys.sort(Comparator.comparingInt(PerfRecord.PHASE::toInt));
        for (PerfRecord.PHASE phase : keys) {
            SumPerfRecord4Print sumPerfRecord = perfRecordMaps4print.get(phase);
            if (sumPerfRecord == null) {
                continue;
            }
            long averageTime = sumPerfRecord.getAverageTime();
            long maxTime = sumPerfRecord.getMaxTime();
            int maxTaskId = sumPerfRecord.maxTaskId;
            int maxTaskGroupId = sumPerfRecord.getMaxTaskGroupId();
            info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %-100s%n",
                    phase, unitTime(averageTime), sumPerfRecord.totalCount, unitTime(maxTime), jobId + "-" + maxTaskGroupId + "-" + maxTaskId, taskDetails.get(maxTaskId)));
        }

        SumPerfRecord4Print countSumPerf = perfRecordMaps4print.get(PerfRecord.PHASE.READ_TASK_DATA);
        if (countSumPerf == null) {
            countSumPerf = new SumPerfRecord4Print();
        }

        long averageRecords = countSumPerf.getAverageRecords();
        long averageBytes = countSumPerf.getAverageBytes();
        long maxRecord = countSumPerf.getMaxRecord();
        long maxByte = countSumPerf.getMaxByte();
        int maxTaskId4Records = countSumPerf.getMaxTaskId4Records();
        int maxTGID4Records = countSumPerf.getMaxTGID4Records();

        info.append("%n%n 2. record average count and max count task info :%n%n");
        info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %18s | %-100s%n", "PHASE", "AVERAGE RECORDS", "AVERAGE BYTES", "MAX RECORDS", "MAX RECORD`S BYTES", "MAX TASK ID", "MAX TASK INFO"));
        if (maxTaskId4Records > -1) {
            info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %18s | %-100s%n"
                    , PerfRecord.PHASE.READ_TASK_DATA, averageRecords, unitSize(averageBytes), maxRecord, unitSize(maxByte), jobId + "-" + maxTGID4Records + "-" + maxTaskId4Records, taskDetails.get(maxTaskId4Records)));
        }
        return info.toString();
    }

    public boolean isEnable()
    {
        return enable;
    }

    public boolean isJob()
    {
        return isJob;
    }

    public void setJobInfo(Configuration jobInfo, boolean perfReportEnable)
    {
        try {
            if (jobInfo != null && perfReportEnable) {

                String jobIdStr = jobInfo.getString("jobId");
                jobId = StringUtils.isEmpty(jobIdStr) ? (long) -5 : Long.parseLong(jobIdStr);
            }
            this.perfReportEnable = perfReportEnable;
        }
        catch (Exception e) {
            this.perfReportEnable = false;
        }
    }

    public long getInstId()
    {
        return instId;
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    private void sumPerf4EndPrint(List<PerfRecord> totalEndReport)
    {
        if (!enable || totalEndReport == null) {
            return;
        }

        for (PerfRecord perfRecord : totalEndReport) {
            perfRecordMaps4print.putIfAbsent(perfRecord.getPhase(), new SumPerfRecord4Print());
            perfRecordMaps4print.get(perfRecord.getPhase()).add(perfRecord);
        }

        totalEndReport.clear();
    }

    public static class SumPerf4Report
    {
        long totalTaskRunTimeInMs = 0L;
        long odpsCloseTimeInMs = 0L;
        long sqlQueryTimeInMs = 0L;
        long resultNextTimeInMs = 0L;

        public void add(long curNanoTime, PerfRecord perfRecord)
        {
            try {
                long runTimeEndInMs;
                if (perfRecord.getElapsedTimeInNs() == -1) {
                    runTimeEndInMs = (curNanoTime - perfRecord.getStartTimeInNs()) / 1000000;
                }
                else {
                    runTimeEndInMs = perfRecord.getElapsedTimeInNs() / 1000000;
                }
                switch (perfRecord.getPhase()) {

                    case TASK_TOTAL:
                        totalTaskRunTimeInMs += runTimeEndInMs;
                        break;
                    case SQL_QUERY:
                        sqlQueryTimeInMs += runTimeEndInMs;
                        break;
                    case RESULT_NEXT_ALL:
                        resultNextTimeInMs += runTimeEndInMs;
                        break;
                    case ODPS_BLOCK_CLOSE:
                        odpsCloseTimeInMs += runTimeEndInMs;
                        break;
                    default:
                        LOG.warn("unknown phase");
                        break;
                }
            }
            catch (Exception e) {
                //do nothing
            }
        }
    }

    public static class SumPerfRecord4Print
    {
        private long perfTimeTotal = 0;
        private long averageTime = 0;
        private long maxTime = 0;
        private int maxTaskId = -1;
        private int maxTaskGroupId = -1;
        private int totalCount = 0;

        private long recordsTotal = 0;
        private long sizesTotal = 0;
        private long averageRecords = 0;
        private long averageBytes = 0;
        private long maxRecord = 0;
        private long maxByte = 0;
        private int maxTaskId4Records = -1;
        private int maxTGID4Records = -1;

        public void add(PerfRecord perfRecord)
        {
            if (perfRecord == null) {
                return;
            }
            perfTimeTotal += perfRecord.getElapsedTimeInNs();
            if (perfRecord.getElapsedTimeInNs() >= maxTime) {
                maxTime = perfRecord.getElapsedTimeInNs();
                maxTaskId = perfRecord.getTaskId();
                maxTaskGroupId = perfRecord.getTaskGroupId();
            }

            recordsTotal += perfRecord.getCount();
            sizesTotal += perfRecord.getSize();
            if (perfRecord.getCount() >= maxRecord) {
                maxRecord = perfRecord.getCount();
                maxByte = perfRecord.getSize();
                maxTaskId4Records = perfRecord.getTaskId();
                maxTGID4Records = perfRecord.getTaskGroupId();
            }

            totalCount++;
        }

        public long getAverageTime()
        {
            if (totalCount > 0) {
                averageTime = perfTimeTotal / totalCount;
            }
            return averageTime;
        }

        public long getMaxTime()
        {
            return maxTime;
        }

        public int getMaxTaskGroupId()
        {
            return maxTaskGroupId;
        }

        public long getAverageRecords()
        {
            if (totalCount > 0) {
                averageRecords = recordsTotal / totalCount;
            }
            return averageRecords;
        }

        public long getAverageBytes()
        {
            if (totalCount > 0) {
                averageBytes = sizesTotal / totalCount;
            }
            return averageBytes;
        }

        public long getMaxRecord()
        {
            return maxRecord;
        }

        public long getMaxByte()
        {
            return maxByte;
        }

        public int getMaxTaskId4Records()
        {
            return maxTaskId4Records;
        }

        public int getMaxTGID4Records()
        {
            return maxTGID4Records;
        }
    }
}
