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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liqiang on 15/11/12.
 */
public class VMInfo
{
    static final long MB = 1024L * 1024L;
    static final long GB = 1024 * 1024 * 1024L;
    private static final Logger LOG = LoggerFactory.getLogger(VMInfo.class);
    private static VMInfo vmInfo;
    // 数据的MxBean
    private final OperatingSystemMXBean osMXBean;
    private final RuntimeMXBean runtimeMXBean;
    private final List<GarbageCollectorMXBean> garbageCollectorMXBeanList;
    private final List<MemoryPoolMXBean> memoryPoolMXBeanList;
    /**
     * 静态信息
     */
    private final String osInfo;
    private final String jvmInfo;
    /**
     * cpu个数
     */
    private final int totalProcessorCount;
    /**
     * 机器的各个状态，用于中间打印和统计上报
     */
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
        //初始化静态信息
        osMXBean = ManagementFactory.getOperatingSystemMXBean();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        garbageCollectorMXBeanList = ManagementFactory.getGarbageCollectorMXBeans();
        memoryPoolMXBeanList = ManagementFactory.getMemoryPoolMXBeans();

        jvmInfo = runtimeMXBean.getVmVendor() + " " + runtimeMXBean.getSpecVersion() + " " + runtimeMXBean.getVmVersion();
        osInfo = osMXBean.getName() + " " + osMXBean.getArch() + " " + osMXBean.getVersion();
        totalProcessorCount = osMXBean.getAvailableProcessors();

        //构建startPhyOSStatus
        startPhyOSStatus = new PhyOSStatus();
        LOG.info("VMInfo# operatingSystem class => {}", osMXBean.getClass().getName());
        if (VMInfo.isSunOsMBean(osMXBean)) {
            {
                startPhyOSStatus.totalPhysicalMemory = VMInfo.getLongFromOperatingSystem(osMXBean, "getTotalPhysicalMemorySize");
                startPhyOSStatus.freePhysicalMemory = VMInfo.getLongFromOperatingSystem(osMXBean, "getFreePhysicalMemorySize");
                startPhyOSStatus.maxFileDescriptorCount = VMInfo.getLongFromOperatingSystem(osMXBean, "getMaxFileDescriptorCount");
                startPhyOSStatus.currentOpenFileDescriptorCount = VMInfo.getLongFromOperatingSystem(osMXBean, "getOpenFileDescriptorCount");
            }
        }

        // 初始化processGCStatus
        for (GarbageCollectorMXBean garbage : garbageCollectorMXBeanList) {
            GCStatus gcStatus = new GCStatus();
            gcStatus.name = garbage.getName();
            processGCStatus.gcStatusMap.put(garbage.getName(), gcStatus);
        }

        // 初始化processMemoryStatus
        if (memoryPoolMXBeanList != null && !memoryPoolMXBeanList.isEmpty()) {
            for (MemoryPoolMXBean pool : memoryPoolMXBeanList) {
                MemoryStatus memoryStatus = new MemoryStatus();
                memoryStatus.name = pool.getName();
                memoryStatus.initSize = pool.getUsage().getInit();
                memoryStatus.maxSize = pool.getUsage().getMax();
                processMemoryStatus.memoryStatusMap.put(pool.getName(), memoryStatus);
            }
        }
    }

    /**
     * static method, get vm information and returns
     *
     * @return null or vmInfo. null is something error, job no care it.
     */
    public static synchronized VMInfo getVmInfo()
    {
        if (vmInfo == null) {
            vmInfo = new VMInfo();
        }
        return vmInfo;
    }

    public static boolean isSunOsMBean(OperatingSystemMXBean operatingSystem)
    {
        final String className = operatingSystem.getClass().getName();

        return "com.sun.management.UnixOperatingSystem".equals(className);
    }

    public static long getLongFromOperatingSystem(OperatingSystemMXBean operatingSystem, String methodName)
    {
        try {
            final Method method = operatingSystem.getClass().getMethod(methodName, (Class<?>[]) null);
            method.setAccessible(true);
            return (Long) method.invoke(operatingSystem, (Object[]) null);
        }
        catch (final Exception e) {
            LOG.info("OperatingSystemMXBean {} failed, Exception = {} ", methodName, e.getMessage());
        }

        return -1;
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
            if (VMInfo.isSunOsMBean(osMXBean)) {
                long curUptime = runtimeMXBean.getUptime();
                long curProcessTime = getLongFromOperatingSystem(osMXBean, "getProcessCpuTime");
                //百分比， uptime是ms，processTime是nano
                if ((curUptime > lastUpTime) && (curProcessTime >= lastProcessCpuTime)) {
                    float curDeltaCpu = (float) (curProcessTime - lastProcessCpuTime) / ((curUptime - lastUpTime) * totalProcessorCount * 10000);
                    processCpuStatus.setMaxMinCpu(curDeltaCpu);
                    processCpuStatus.averageCpu = (float) curProcessTime / (curUptime * totalProcessorCount * 10000);

                    lastUpTime = curUptime;
                    lastProcessCpuTime = curProcessTime;
                }
            }

            for (GarbageCollectorMXBean garbage : garbageCollectorMXBeanList) {

                GCStatus gcStatus = processGCStatus.gcStatusMap.get(garbage.getName());
                if (gcStatus == null) {
                    gcStatus = new GCStatus();
                    gcStatus.name = garbage.getName();
                    processGCStatus.gcStatusMap.put(garbage.getName(), gcStatus);
                }

                long curTotalGcCount = garbage.getCollectionCount();
                gcStatus.setCurTotalGcCount(curTotalGcCount);

                long curtotalGcTime = garbage.getCollectionTime();
                gcStatus.setCurTotalGcTime(curtotalGcTime);
            }

            if (memoryPoolMXBeanList != null && !memoryPoolMXBeanList.isEmpty()) {
                for (MemoryPoolMXBean pool : memoryPoolMXBeanList) {

                    MemoryStatus memoryStatus = processMemoryStatus.memoryStatusMap.get(pool.getName());
                    if (memoryStatus == null) {
                        memoryStatus = new MemoryStatus();
                        memoryStatus.name = pool.getName();
                        processMemoryStatus.memoryStatusMap.put(pool.getName(), memoryStatus);
                    }
                    memoryStatus.commitedSize = pool.getUsage().getCommitted();
                    memoryStatus.setMaxMinUsedSize(pool.getUsage().getUsed());
                    long maxMemory = memoryStatus.commitedSize > 0 ? memoryStatus.commitedSize : memoryStatus.maxSize;
                    memoryStatus.setMaxMinPercent(maxMemory > 0 ? (float) 100 * memoryStatus.usedSize / maxMemory : -1);
                }
            }

            if (print) {
                LOG.info("{}{}{}", processCpuStatus.getDeltaString(), processMemoryStatus.getDeltaString(), processGCStatus.getDeltaString());
            }
        }
        catch (Exception e) {
            LOG.warn("no need care, the fail is ignored : vmInfo getDelta failed {}", e.getMessage(), e);
        }
    }

    private static class PhyOSStatus
    {
        long totalPhysicalMemory = -1;
        long freePhysicalMemory = -1;
        long maxFileDescriptorCount = -1;
        long currentOpenFileDescriptorCount = -1;

        public String toString()
        {
            return String.format("\ttotalPhysicalMemory:\t%,.2fG%n"
                            + "\tfreePhysicalMemory:\t%,.2fG%n"
                            + "\tmaxFileDescriptorCount:\t%s%n"
                            + "\tcurrentOpenFileDescriptorCount:\t%s%n",
                    (float) totalPhysicalMemory / GB, (float) freePhysicalMemory / GB, maxFileDescriptorCount, currentOpenFileDescriptorCount);
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
                        gc.name, gc.curDeltaGCCount, gc.totalGCCount, gc.maxDeltaGCCount, gc.minDeltaGCCount,
                        (float) gc.curDeltaGCTime / 1000,
                        (float) gc.totalGCTime / 1000,
                        (float) gc.maxDeltaGCTime / 1000,
                        (float) gc.minDeltaGCTime / 1000));
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
                        gc.name, gc.totalGCCount, gc.maxDeltaGCCount, gc.minDeltaGCCount,
                        String.format("%,.3fs", (float) gc.totalGCTime / 1000),
                        String.format("%,.3fs", (float) gc.maxDeltaGCTime / 1000),
                        String.format("%,.3fs", (float) gc.minDeltaGCTime / 1000)));
            }
            return sb.toString();
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
                sb.append(String.format("%-30s | %,-14.2f | %,-8.2f %n", ms.name, (float) ms.maxSize / MB, (float) ms.initSize / MB));
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
                        ms.name, (float) ms.usedSize / MB, ms.percent, (float) ms.maxUsedSize / MB, ms.maxPercent));
            }
            return sb.toString();
        }
    }

    private static class GCStatus
    {
        String name;
        long maxDeltaGCCount = -1;
        long minDeltaGCCount = -1;
        long curDeltaGCCount;
        long totalGCCount = 0;
        long maxDeltaGCTime = -1;
        long minDeltaGCTime = -1;
        long curDeltaGCTime;
        long totalGCTime = 0;

        public void setCurTotalGcCount(long curTotalGcCount)
        {
            this.curDeltaGCCount = curTotalGcCount - totalGCCount;
            this.totalGCCount = curTotalGcCount;

            if (maxDeltaGCCount < curDeltaGCCount) {
                maxDeltaGCCount = curDeltaGCCount;
            }

            if (minDeltaGCCount == -1 || minDeltaGCCount > curDeltaGCCount) {
                minDeltaGCCount = curDeltaGCCount;
            }
        }

        public void setCurTotalGcTime(long curTotalGcTime)
        {
            this.curDeltaGCTime = curTotalGcTime - totalGCTime;
            this.totalGCTime = curTotalGcTime;

            if (maxDeltaGCTime < curDeltaGCTime) {
                maxDeltaGCTime = curDeltaGCTime;
            }

            if (minDeltaGCTime == -1 || minDeltaGCTime > curDeltaGCTime) {
                minDeltaGCTime = curDeltaGCTime;
            }
        }
    }

    private static class MemoryStatus
    {
        String name;
        long initSize;
        long maxSize;
        long commitedSize;
        long usedSize;
        float percent;
        long maxUsedSize = -1;
        float maxPercent = 0;

        void setMaxMinUsedSize(long curUsedSize)
        {
            if (maxUsedSize < curUsedSize) {
                maxUsedSize = curUsedSize;
            }
            this.usedSize = curUsedSize;
        }

        void setMaxMinPercent(float curPercent)
        {
            if (maxPercent < curPercent) {
                maxPercent = curPercent;
            }
            this.percent = curPercent;
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

            return "\n [delta cpu info] => \n" +
                    "\t" +
                    String.format("%-11s | %-10s | %-11s | %-11s %n", "curDeltaCPU", "averageCPU", "maxDeltaCPU", "minDeltaCPU") +
                    "\t" +
                    String.format("%-11s | %-10s | %-11s | %-11s %n",
                            String.format("%,.2f%%", processCpuStatus.curDeltaCpu),
                            String.format("%,.2f%%", processCpuStatus.averageCpu),
                            String.format("%,.2f%%", processCpuStatus.maxDeltaCpu),
                            String.format("%,.2f%%%n", processCpuStatus.minDeltaCpu));
        }

        public String getTotalString()
        {

            return "\n [total cpu info] => \n" +
                    "\t" +
                    String.format("%-10s | %-11s | %-11s %n", "averageCPU", "maxDeltaCPU", "minDeltaCPU") +
                    "\t" +
                    String.format("%-10s | %-11s | %-11s %n",
                            String.format("%,.2f%%", processCpuStatus.averageCpu),
                            String.format("%,.2f%%", processCpuStatus.maxDeltaCpu),
                            String.format("%,.2f%%%n", processCpuStatus.minDeltaCpu));
        }
    }
}
