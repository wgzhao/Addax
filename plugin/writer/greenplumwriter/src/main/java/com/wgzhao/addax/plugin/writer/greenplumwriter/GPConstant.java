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

package com.wgzhao.addax.plugin.writer.greenplumwriter;

import com.wgzhao.addax.core.base.Constant;

public class GPConstant
        extends Constant
{
    public static final char DELIMITER = '\u0001';
    public static final char QUOTE_CHAR = '\u0002';
    public static final char NEWLINE = '\n';
    public static final char ESCAPE = '\\';
    // https://gpdb.docs.pivotal.io/5100/admin_guide/load/topics/g-copy-encoding.html
    public static final int MAX_CSV_SIZE = 4194304;
}
