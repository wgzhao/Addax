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

package com.wgzhao.addax.plugin.writer.hbase11xwriter;

import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.hadoop.hbase.client.Put;

public class MultiVersionTask
        extends HbaseAbstractTask
{

    public MultiVersionTask(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    public Put convertRecordToPut(Record record)
    {
        if (record.getColumnNumber() != 4) {
            // multiversion 模式下源头读取字段列数为4元组(rowkey,column,timestamp,value),目的端需告诉[]
            throw AddaxException
                    .asAddaxException(
                            Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "The record should be a tuple of (rowkey,column,timestamp,value) in multiversion mode. actually get %s",
                                    record.getColumnNumber()));
        }
        Put put = null;
        return put;
    }
}
