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

package com.wgzhao.addax.core.statistics.container.communicator;

import com.wgzhao.addax.common.statistics.VMInfo;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.meta.State;
import com.wgzhao.addax.core.statistics.communication.Communication;
import com.wgzhao.addax.core.statistics.container.collector.AbstractCollector;
import com.wgzhao.addax.core.statistics.container.report.AbstractReporter;
import com.wgzhao.addax.core.util.container.CoreConstant;

import java.util.List;
import java.util.Map;

public abstract class AbstractContainerCommunicator
{
    private final Configuration configuration;
    private final Long jobId;
    private final VMInfo vmInfo = VMInfo.getVmInfo();
    private AbstractCollector collector;
    private AbstractReporter reporter;
    private long lastReportTime = System.currentTimeMillis();

    public AbstractContainerCommunicator(Configuration configuration)
    {
        this.configuration = configuration;
        this.jobId = configuration.getLong(CoreConstant.ADDAX_CORE_CONTAINER_JOB_ID);
    }

    public Configuration getConfiguration()
    {
        return this.configuration;
    }

    public AbstractCollector getCollector()
    {
        return collector;
    }

    public void setCollector(AbstractCollector collector)
    {
        this.collector = collector;
    }

    public AbstractReporter getReporter()
    {
        return reporter;
    }

    public void setReporter(AbstractReporter reporter)
    {
        this.reporter = reporter;
    }

    public Long getJobId()
    {
        return jobId;
    }

    public abstract void registerCommunication(List<Configuration> configurationList);

    public abstract Communication collect();

    public abstract void report(Communication communication);

    public abstract State collectState();

    public abstract Communication getCommunication(Integer id);

    /*
     * 当 实现是 TGContainerCommunicator 时，返回的 Map: key=taskId, value=Communication
     * 当 实现是 JobContainerCommunicator 时，返回的 Map: key=taskGroupId, value=Communication
     * @return map
     */
    public abstract Map<Integer, Communication> getCommunicationMap();

    public void resetCommunication(Integer id)
    {
        Map<Integer, Communication> map = getCommunicationMap();
        map.put(id, new Communication());
    }

    public void reportVmInfo()
    {
        long now = System.currentTimeMillis();
        //每5分钟打印一次
        if (now - lastReportTime >= 300000) {
            //当前仅打印
            if (vmInfo != null) {
                vmInfo.getDelta(true);
            }
            lastReportTime = now;
        }
    }
}