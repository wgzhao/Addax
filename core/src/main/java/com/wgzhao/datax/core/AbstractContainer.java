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

package com.wgzhao.datax.core;

import com.wgzhao.datax.common.util.Configuration;
import com.wgzhao.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import org.apache.commons.lang3.Validate;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer
{
    protected Configuration configuration;

    protected AbstractContainerCommunicator containerCommunicator;

    public AbstractContainer(Configuration configuration)
    {

        Validate.notNull(configuration, "Configuration can not be null.");

        this.configuration = configuration;
    }

    public Configuration getConfiguration()
    {
        return configuration;
    }

    public AbstractContainerCommunicator getContainerCommunicator()
    {
        return containerCommunicator;
    }

    public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator)
    {
        this.containerCommunicator = containerCommunicator;
    }

    public abstract void start();
}
