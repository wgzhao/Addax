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

package com.wgzhao.addax.common.spi;

import com.wgzhao.addax.common.base.BaseObject;
import com.wgzhao.addax.common.plugin.AbstractJobPlugin;
import com.wgzhao.addax.common.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.util.Configuration;

import java.util.List;

public abstract class Writer
        extends BaseObject
{
    public abstract static class Job
            extends AbstractJobPlugin
    {

        /**
         * split the task
         *
         * @param mandatoryNumber the number of tasks that the framework suggests the plugin to split
         * @return list of configuration
         */
        public abstract List<Configuration> split(int mandatoryNumber);
    }

    public abstract static class Task
            extends AbstractTaskPlugin
    {

        public abstract void startWrite(RecordReceiver lineReceiver);

        public boolean supportFailOver() {return false;}
    }
}
