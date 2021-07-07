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

package com.wgzhao.addax.plugin.reader.hdfsreader;

import java.util.Arrays;
import java.util.List;

/**
 * Created by mingya.wmy on 2015/8/14.
 */
public class Constant
{

    public static final String SOURCE_FILES = "sourceFiles";
    public static final String TEXT = "TEXT";
    public static final String ORC = "ORC";
    public static final String CSV = "CSV";
    public static final String SEQ = "SEQ";
    public static final String RC = "RC";
    public static final String PARQUET = "PARQUET";
    protected static final List<String> SUPPORT_FILE_TYPE = Arrays.asList(Constant.CSV, Constant.ORC, Constant.RC, Constant.SEQ, Constant.TEXT, Constant.PARQUET);
    private Constant() {}
}
