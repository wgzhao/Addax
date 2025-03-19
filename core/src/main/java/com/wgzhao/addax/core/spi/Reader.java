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

package com.wgzhao.addax.core.spi;

import com.wgzhao.addax.core.base.BaseObject;
import com.wgzhao.addax.core.plugin.AbstractJobPlugin;
import com.wgzhao.addax.core.plugin.AbstractTaskPlugin;
import com.wgzhao.addax.core.plugin.RecordSender;
import com.wgzhao.addax.core.util.Configuration;

import java.util.List;

public abstract class Reader
        extends BaseObject
{

    public abstract static class Job
            extends AbstractJobPlugin
    {

        /**
         * split the task
         *
         * @param adviceNumber the number of tasks that the framework suggests the plugin to split
         * The reason for this suggestion is to provide the best implementation for the user.
         * For example, the framework calculates that the user's data storage can support 100 concurrent connections,
         * and the user needs 100 concurrent connections. If the plugin developer can split the tasks according to the
         * above rules and achieve more than 100 connections, Addax can start 100 Channels simultaneously, providing
         * the best throughput for the user.
         * For example, if the user is synchronizing a single MySQL table and expects a throughput of 10 concurrent connections,
         * the plugin developer should split the table, such as by using primary key ranges. If the final number of split tasks is
         * greater than or equal to 10, we can provide the user with the maximum throughput.
         * * Of course, this is just a suggested value. The Reader plugin can split according to its own rules, but we recommend
         * splitting according to the framework's suggested value.
         * @return list of configuration
         */
        public abstract List<Configuration> split(int adviceNumber);
    }

    public abstract static class Task
            extends AbstractTaskPlugin
    {
        public abstract void startRead(RecordSender recordSender);
    }
}
