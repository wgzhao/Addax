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

package com.wgzhao.addax.plugin.reader.hbase20xreader;

import com.wgzhao.addax.core.base.HBaseKey;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class MultiVersionDynamicColumnTask
        extends MultiVersionTask
{
    private final List<String> columnFamilies;

    public MultiVersionDynamicColumnTask(Configuration configuration)
    {
        super(configuration);

        this.columnFamilies = configuration.getList(HBaseKey.COLUMN_FAMILY, String.class);
    }

    @Override
    public void initScan(Scan scan)
    {
        for (String columnFamily : columnFamilies) {
            scan.addFamily(Bytes.toBytes(columnFamily.trim()));
        }

        super.setMaxVersions(scan);
    }
}
