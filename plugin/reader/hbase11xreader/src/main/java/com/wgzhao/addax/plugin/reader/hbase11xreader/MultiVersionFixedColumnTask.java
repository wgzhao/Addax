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

package com.wgzhao.addax.plugin.reader.hbase11xreader;

import com.wgzhao.addax.common.base.HBaseKey;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class MultiVersionFixedColumnTask
        extends MultiVersionTask
{

    public MultiVersionFixedColumnTask(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    public void initScan(Scan scan)
    {
        for (Map<String, String> aColumn : column) {
            String columnName = aColumn.get(HBaseKey.NAME);
            if (!Hbase11xHelper.isRowkeyColumn(columnName)) {
                String[] cfAndQualifier = columnName.split(":");
                scan.addColumn(Bytes.toBytes(cfAndQualifier[0].trim()), Bytes.toBytes(cfAndQualifier[1].trim()));
            }
        }
        super.setMaxVersions(scan);
    }
}
