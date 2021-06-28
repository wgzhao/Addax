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

package com.wgzhao.datax.plugin.writer.dbffilewriter;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class Key
{

    private Key() {}

    // must have
    public static final String PATH = "path";
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String LENGTH = "length";
    public static final String SCALE = "scale";
    public static final String FORMAT = "format";
    public static final String DATE_FORMAT = "dateFormat";
    public static final String FILE_NAME = "fileName";
    public static final String WRITE_MODE = "writeMode";
    public static final String ENCODING = "encoding";
}
