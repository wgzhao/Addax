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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VMInfo
{
    static final long MB = 1024L * 1024L;
    static final long GB = 1024 * 1024 * 1024L;
    private static final Logger LOG = LoggerFactory.getLogger(VMInfo.class);
    private static VMInfo vmInfo;

    private final List<GarbageCollectorMXBean> garbageCollectorMXBeanList;
    private final List<MemoryPoolMXBean> memoryPoolMXBeanList;
    private final RuntimeMXBean runtimeMXBean;
    private final ThreadMXBean threadMXBean;

    private final String osInfo;
    private final String jvmInfo;
    private final int totalProcessorCount;

    // machine status
    private final PhyOSStatus startPhyOSStatus;
    private final ProcessCpuStatus processCpuStatus = new ProcessCpuStatus();
    private final ProcessGCStatus processGCStatus = new ProcessGCStatus();
    private final ProcessMemoryStatus processMemoryStatus = new ProcessMemoryStatus();
    //ms
    private long lastUpTime = 0;
    //nano
    private long lastProcessCpuTime = 0;

    private VMInfo()
    {
        Runtime runtime = Runtime.getRuntime();
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        this.runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        this.threadMXBean = ManagementFactory.getThreadMXBean();
        garbageCollectorMXBeanList = ManagementFactory.getGarbageCollectorMXBeans();
        memoryPoolMXBeanList = ManagementFactory.getMemoryPoolMXBeans();

        jvmInfo = runtimeMXBean.getVmVendor() + " " + runtimeMXBean.getSpecVersion() + " " + runtimeMXBean.getVmVersion();
        osInfo = osMXBean.getName() + " " + osMXBean.getArch() + " " + osMXBean.getVersion();
        totalProcessorCount = osMXBean.getAvailableProcessors();

        startPhyOSStatus = new PhyOSStatus(
                runtime.totalMemory(),
                runtime.freeMemory(),
                -1L, // File descriptor count not available in standard API
                -1L  // Open file count not available in standard API
        );

        // initialize processGCStatus
        for (GarbageCollectorMXBean garbage : garbageCollectorMXBeanList) {
            GCStatus gcStatus = new GCStatus.Builder()
                    .name(garbage.getName())
                    .build();
            processGCStatus.gcStatusMap.put(garbage.getName(), gcStatus);
        }

        // initialize processMemoryStatus
        updateMemoryStatuses();
    }

    public static synchronized VMInfo getVmInfo()
    {
        if (vmInfo == null) {
            vmInfo = new VMInfo();
        }
        return vmInfo;
    }

    public String toString()
    {
        return "The machine info  => \n\n"
                + "\tosInfo: \t" + osInfo + "\n"
                + "\tjvmInfo:\t" + jvmInfo + "\n"
                + "\tcpu num:\t" + totalProcessorCount + "\n\n"
                + startPhyOSStatus.toString() + "\n"
                + processGCStatus + "\n"
                + processMemoryStatus + "\n";
    }

    public String totalString()
    {
        return (processCpuStatus.getTotalString() + processGCStatus.getTotalString());
    }

    public synchronized void getDelta(boolean print)
    {

        try {
            long curUptime = runtimeMXBean.getUptime();
            // Get CPU time by summing all thread CPU times
            long curProcessTime = getTotalThreadCpuTime();

            // CPU load calculation
            if ((curUptime > lastUpTime) && (curProcessTime >= lastProcessCpuTime)) {
                long deltaUptime = curUptime - lastUpTime;
                long deltaProcessTime = curProcessTime - lastProcessCpuTime;

                float cpu = ((float) deltaProcessTime / deltaUptime / totalProcessorCount) * 100;
                processCpuStatus.setMaxMinCpu(cpu);
                processCpuStatus.averageCpu = cpu;
            }
            this.lastUpTime = curUptime;
            this.lastProcessCpuTime = curProcessTime;

            // Update GC statistics
            for (GarbageCollectorMXBean garbage : garbageCollectorMXBeanList) {
                GCStatus gcStatus = processGCStatus.gcStatusMap.get(garbage.getName());
                if (gcStatus == null) {
                    gcStatus = new GCStatus.Builder()
                            .name(garbage.getName())
                            .build();
                    processGCStatus.gcStatusMap.put(garbage.getName(), gcStatus);
                }

                GCStatus.Builder builder = new GCStatus.Builder()
                        .name(gcStatus.name)
                        .setCurTotalGcCount(garbage.getCollectionCount())
                        .setCurTotalGcTime(garbage.getCollectionTime());

                processGCStatus.gcStatusMap.put(garbage.getName(), builder.build());
            }

            updateMemoryStatuses();

            if (print) {
                LOG.info("{}{}{}",
                        processCpuStatus.getDeltaString(),
                        processMemoryStatus.getDeltaString(),
                        processGCStatus.getDeltaString()
                );
            }
        }
        catch (Exception e) {
            LOG.warn("VMInfo getDelta failed: {}", e.getMessage(), e);
        }
    }

    private long getTotalThreadCpuTime()
    {
        long totalCpuTime = 0;
        if (threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
            for (long threadId : threadMXBean.getAllThreadIds()) {
                long cpuTime = threadMXBean.getThreadCpuTime(threadId);
                if (cpuTime != -1) {
                    totalCpuTime += cpuTime;
                }
            }
        }
        return totalCpuTime;
    }

    private void updateMemoryStatuses()
    {
        if (memoryPoolMXBeanList != null && !memoryPoolMXBeanList.isEmpty()) {
            memoryPoolMXBeanList.forEach(pool -> {
                var memoryStatus = processMemoryStatus.memoryStatusMap.get(pool.getName());
                if (memoryStatus == null) {
                    memoryStatus = MemoryStatus.create(
                            pool.getName(),
                            pool.getUsage().getInit(),
                            pool.getUsage().getMax()
                    );
                    processMemoryStatus.memoryStatusMap.put(pool.getName(), memoryStatus);
                }

                processMemoryStatus.memoryStatusMap.put(
                        pool.getName(),
                        memoryStatus.withUsage(
                                pool.getUsage().getUsed(),
                                pool.getUsage().getCommitted()
                        )
                );
            });
        }
    }

    private record PhyOSStatus(
            long totalPhysicalMemory,
            long freePhysicalMemory,
            long maxFileDescriptorCount,
            long currentOpenFileDescriptorCount
    )
    {
        public String toString()
        {
            return """
                    \ttotalPhysicalMemory:\t%,.2fG
                    \tfreePhysicalMemory:\t%,.2fG
                    \tmaxFileDescriptorCount:\t%s
                    \tcurrentOpenFileDescriptorCount:\t%s
                    """.formatted(
                    (float) totalPhysicalMemory / GB,
                    (float) freePhysicalMemory / GB,
                    maxFileDescriptorCount,
                    currentOpenFileDescriptorCount
            );
        }
    }

    private static class ProcessGCStatus
    {
        final Map<String, GCStatus> gcStatusMap = new HashMap<>();

        public String toString()
        {
            return "\tGC Names\t" + gcStatusMap.keySet() + "\n";
        }

        public String getDeltaString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n [delta gc info] => \n");
            sb.append("\t");
            sb.append(String.format("%-16s | %-15s | %-12s | %-15s | %-15s | %-14s | %-14s | %-11s | %-14s %n",
                    "NAME", "curDeltaGCCount", "totalGCCount", "maxDeltaGCCount", "minDeltaGCCount", "curDeltaGCTime",
                    "totalGCTime", "maxDeltaGCTime", "minDeltaGCTime"));
            for (GCStatus gc : gcStatusMap.values()) {
                sb.append("\t");
                sb.append(String.format("%-16s | %-15s | %-12s | %-15s | %-15s | %,-14.3f | %,-14.3f | %,-11.3f | %,-14.3f %n",
                        gc.name(), gc.curDeltaGCCount(), gc.totalGCCount(), gc.maxDeltaGCCount(), gc.minDeltaGCCount(),
                        (float) gc.curDeltaGCTime() / 1000,
                        (float) gc.totalGCTime() / 1000,
                        (float) gc.maxDeltaGCTime() / 1000,
                        (float) gc.minDeltaGCTime() / 1000));
            }
            return sb.toString();
        }

        public String getTotalString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n [total gc info] => \n");
            sb.append("\t");
            sb.append(String.format("%-16s | %-12s | %-15s | %-15s | %-11s | %-14s | %-14s %n",
                    "NAME", "totalGCCount", "maxDeltaGCCount", "minDeltaGCCount", "totalGCTime", "maxDeltaGCTime", "minDeltaGCTime"));
            for (GCStatus gc : gcStatusMap.values()) {
                sb.append("\t");
                sb.append(String.format("%-16s | %-12s | %-15s | %-15s | %-11s | %-14s | %-14s %n",
                        gc.name(), gc.totalGCCount(), gc.maxDeltaGCCount(), gc.minDeltaGCCount(),
                        String.format("%,.3fs", (float) gc.totalGCTime() / 1000),
                        String.format("%,.3fs", (float) gc.maxDeltaGCTime() / 1000),
                        String.format("%,.3fs", (float) gc.minDeltaGCTime() / 1000)));
            }
            return sb.toString();
        }
    }

    private record GCStatus(
            String name,
            long maxDeltaGCCount,
            long minDeltaGCCount,
            long curDeltaGCCount,
            long totalGCCount,
            long maxDeltaGCTime,
            long minDeltaGCTime,
            long curDeltaGCTime,
            long totalGCTime
    )
    {
        public static class Builder
        {
            private String name;
            private long maxDeltaGCCount = -1;
            private long minDeltaGCCount = -1;
            private long curDeltaGCCount;
            private long totalGCCount = 0;
            private long maxDeltaGCTime = -1;
            private long minDeltaGCTime = -1;
            private long curDeltaGCTime;
            private long totalGCTime = 0;

            public Builder name(String name)
            {
                this.name = name;
                return this;
            }

            public Builder setCurTotalGcCount(long curTotalGcCount)
            {
                this.curDeltaGCCount = curTotalGcCount - totalGCCount;
                this.totalGCCount = curTotalGcCount;

                this.maxDeltaGCCount = Math.max(maxDeltaGCCount, curDeltaGCCount);
                this.minDeltaGCCount = (minDeltaGCCount == -1) ?
                        curDeltaGCCount : Math.min(minDeltaGCCount, curDeltaGCCount);
                return this;
            }

            public Builder setCurTotalGcTime(long curTotalGcTime)
            {
                this.curDeltaGCTime = curTotalGcTime - totalGCTime;
                this.totalGCTime = curTotalGcTime;

                this.maxDeltaGCTime = Math.max(maxDeltaGCTime, curDeltaGCTime);
                this.minDeltaGCTime = (minDeltaGCTime == -1) ?
                        curDeltaGCTime : Math.min(minDeltaGCTime, curDeltaGCTime);
                return this;
            }

            public GCStatus build()
            {
                return new GCStatus(name, maxDeltaGCCount, minDeltaGCCount, curDeltaGCCount,
                        totalGCCount, maxDeltaGCTime, minDeltaGCTime, curDeltaGCTime, totalGCTime);
            }
        }
    }

    private static class ProcessMemoryStatus
    {
        final Map<String, MemoryStatus> memoryStatusMap = new HashMap<>();

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\t");
            sb.append(String.format("%-30s | %-14s | %-8s %n", "MEMORY_NAME", "allocation(MB)", "init(MB)"));
            for (MemoryStatus ms : memoryStatusMap.values()) {
                sb.append("\t");
                sb.append(String.format("%-30s | %,-14.2f | %,-8.2f %n", ms.name(), (float) ms.maxSize() / MB, (float) ms.initSize() / MB));
            }
            return sb.toString();
        }

        public String getDeltaString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n [delta memory info] => \n");
            sb.append("\t");
            sb.append(String.format("%-22s | %-10s | %-12s | %-13s | %-12s %n",
                    "NAME", "used(MB)", "used_pct(%)", "max_used(MB)", "max_pct(%)"));
            for (MemoryStatus ms : memoryStatusMap.values()) {
                sb.append("\t");
                sb.append(String.format("%-22s | %,-10.2f | %,-12.2f | %,-13.2f | %,-12.2f %n",
                        ms.name(), (float) ms.usedSize() / MB, ms.percent(), (float) ms.maxUsedSize() / MB, ms.maxPercent()));
            }
            return sb.toString();
        }
    }

    private record MemoryStatus(
            String name,
            long initSize,
            long maxSize,
            long commitedSize,
            long usedSize,
            float percent,
            long maxUsedSize,
            float maxPercent
    )
    {
        public static MemoryStatus create(String name, long initSize, long maxSize)
        {
            return new MemoryStatus(name, initSize, maxSize, 0, 0, 0, -1, 0);
        }

        public MemoryStatus withUsage(long curUsedSize, long commitedSize)
        {
            long newMaxUsedSize = Math.max(this.maxUsedSize, curUsedSize);
            float curPercent = commitedSize > 0 ?
                    (float) 100 * curUsedSize / commitedSize : -1;
            float newMaxPercent = Math.max(this.maxPercent, curPercent);

            return new MemoryStatus(name, initSize, maxSize, commitedSize,
                    curUsedSize, curPercent, newMaxUsedSize, newMaxPercent);
        }
    }

    private class ProcessCpuStatus
    {
        // 百分比的值 比如30.0 表示30.0%
        float maxDeltaCpu = -1;
        float minDeltaCpu = -1;
        float curDeltaCpu = -1;
        float averageCpu = -1;

        public void setMaxMinCpu(float curCpu)
        {
            this.curDeltaCpu = curCpu;
            if (maxDeltaCpu < curCpu) {
                maxDeltaCpu = curCpu;
            }

            if (minDeltaCpu == -1 || minDeltaCpu > curCpu) {
                minDeltaCpu = curCpu;
            }
        }

        public String getDeltaString()
        {

            return String.format("""
                            
                             [delta cpu info] =>
                            \t%-11s | %-10s | %-11s | %-11s
                            \t%11.2f%% | %10.2f%% | %11.2f%% | %11.2f%%
                            """,
                    "curDeltaCPU", "averageCPU", "maxDeltaCPU", "minDeltaCPU",
                    processCpuStatus.curDeltaCpu,
                    processCpuStatus.averageCpu,
                    processCpuStatus.maxDeltaCpu,
                    processCpuStatus.minDeltaCpu);
        }

        public String getTotalString()
        {
            return String.format("""
                            
                             [total cpu info] =>
                            \t%-10s | %-11s | %-11s
                            \t%10.2f%% | %11.2f%% | %11.2f%%
                            """, "averageCPU", "maxDeltaCPU", "minDeltaCPU",
                    processCpuStatus.averageCpu,
                    processCpuStatus.maxDeltaCpu,
                    processCpuStatus.minDeltaCpu);
        }
    }
}
