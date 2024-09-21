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

package com.wgzhao.addax.plugin.writer.kuduwriter;

import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.common.exception.ErrorCode.CONNECT_ERROR;
import static com.wgzhao.addax.common.exception.ErrorCode.RUNTIME_ERROR;

public class KuduHelper
{

    private static final Logger LOG = LoggerFactory.getLogger(KuduHelper.class);
    private final KuduClient kuduClient;
    private KuduTable kuduTable;

    public KuduHelper(String masterAddress)
    {
        this(masterAddress, 100 * 1000L);
    }

    public KuduHelper(String masterAddress, long timeout)
    {
        try {
            this.kuduClient = new KuduClient.KuduClientBuilder(masterAddress)
                    .defaultOperationTimeoutMs(timeout)
                    .build();
        }
        catch (Exception e) {
            throw AddaxException.asAddaxException(CONNECT_ERROR, e);
        }
    }

    public KuduTable getKuduTable(String tableName)
    {
        if (tableName == null) {
            return null;
        }
        else {
            try {
                kuduTable = kuduClient.openTable(tableName);
                return kuduTable;
            }
            catch (KuduException e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
            }
        }
    }

    public boolean isTableExists(String tableName)
    {
        if (tableName == null) {
            return false;
        }
        try {
            return kuduClient.tableExists(tableName);
        }
        catch (KuduException e) {
            throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
        }
    }

    public void closeClient()
    {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        }
        catch (KuduException e) {
            LOG.warn("The \"kudu client\" was not stopped gracefully. !");
        }
    }

    public Schema getSchema(String tableName)
    {
        if (kuduTable != null) {
            return kuduTable.getSchema();
        }
        else {
            kuduTable = getKuduTable(tableName);
            return kuduTable.getSchema();
        }
    }

    public List<String> getAllColumns(String tableName)
    {
        List<String> columns = new ArrayList<>();
        Schema schema = getSchema(tableName);
        for (ColumnSchema column : schema.getColumns()) {
            columns.add(column.getName());
        }
        return columns;
    }

    public KuduSession getSession()
    {
        return kuduClient.newSession();
    }
}
