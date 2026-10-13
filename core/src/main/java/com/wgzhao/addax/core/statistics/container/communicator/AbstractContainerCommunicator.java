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

import com.wgzhao.addax.core.statistics.VMInfo;
import com.wgzhao.addax.core.util.Configuration;
import com.wgzhao.addax.core.statistics.communication.Communication;

import java.util.List;

/** Abstract Container Communicator. */
public abstract class AbstractContainerCommunicator
{
    private final VMInfo vmInfo = VMInfo.getVmInfo();
    private long lastReportTime = System.currentTimeMillis();

    /** Registercommunication. */
    public abstract void registerCommunication(List<Configuration> configurationList);

    /** Collect. */
    public abstract Communication collect();

    /** Report. */
    public abstract void report(Communication communication);

    /** Reportvminfo. */
    public void reportVmInfo()
    {
        long now = System.currentTimeMillis();
        if (now - lastReportTime >= 300_000) {
            if (vmInfo != null) {
                vmInfo.getDelta(true);
            }
            lastReportTime = now;
        }
    }
}
