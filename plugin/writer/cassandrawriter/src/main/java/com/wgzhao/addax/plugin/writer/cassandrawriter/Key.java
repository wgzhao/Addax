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

package com.wgzhao.addax.plugin.writer.cassandrawriter;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public class Key
{
    public final static String USERNAME = "username";
    public final static String PASSWORD = "password";

    public final static String HOST = "host";
    public final static String PORT = "port";
    public final static String USESSL = "useSSL";

    public final static String KEYSPACE = "keyspace";
    public final static String TABLE = "table";
    public final static String COLUMN = "column";
    public final static String WRITE_TIME = "writetime()";
    public final static String ASYNC_WRITE = "asyncWrite";
    public final static String CONSITANCY_LEVEL = "consistancyLevel";
    public final static String CONNECTIONS_PER_HOST = "connectionsPerHost";
    public final static String MAX_PENDING_CONNECTION = "maxPendingPerConnection";
    /**
     * 异步写入的批次大小，默认1（不异步写入）
     */
    public final static String BATCH_SIZE = "batchSize";
}
