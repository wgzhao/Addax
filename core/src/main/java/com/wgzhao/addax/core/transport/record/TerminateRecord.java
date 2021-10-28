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

package com.wgzhao.addax.core.transport.record;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;

/**
 * 作为标示 生产者已经完成生产的标志
 */
public class TerminateRecord
        implements Record
{
    private static final TerminateRecord SINGLE = new TerminateRecord();

    private TerminateRecord()
    {
    }

    public static TerminateRecord get()
    {
        return SINGLE;
    }

    @Override
    public void addColumn(Column column)
    {
        //
    }

    @Override
    public Column getColumn(int i)
    {
        return null;
    }

    @Override
    public int getColumnNumber()
    {
        return 0;
    }

    @Override
    public int getByteSize()
    {
        return 0;
    }

    @Override
    public int getMemorySize()
    {
        return 0;
    }

    @Override
    public void setColumn(int i, Column column)
    {
        //
    }
}
