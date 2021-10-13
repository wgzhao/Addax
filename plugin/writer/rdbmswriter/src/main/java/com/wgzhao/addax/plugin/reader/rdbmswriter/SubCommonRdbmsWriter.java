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

package com.wgzhao.addax.plugin.reader.rdbmswriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.rdbms.util.DBUtil;
import com.wgzhao.addax.rdbms.util.DataBaseType;
import com.wgzhao.addax.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class SubCommonRdbmsWriter
        extends CommonRdbmsWriter
{
    public static class Job
            extends CommonRdbmsWriter.Job
    {
        public Job(DataBaseType dataBaseType)
        {
            super(dataBaseType);
        }
    }

    public static class Task
            extends CommonRdbmsWriter.Task
    {
        public Task(DataBaseType dataBaseType)
        {
            super(dataBaseType);
        }
    }

    static {
        DBUtil.loadDriverClass("writer", "rdbms");
    }
}
