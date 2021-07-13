/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.storage.writer;

public class Constant
{

    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final char DEFAULT_FIELD_DELIMITER = ',';

//    public static final String DEFAULT_NULL_FORMAT = "\\N"

    public static final String FILE_FORMAT_CSV = "csv";

    public static final String FILE_FORMAT_TEXT = "text";

    //每个分块10MB，最大10000个分块
    public static final Long MAX_FILE_SIZE = 1024 * 1024 * 10 * 10000L;

//    public static final String DEFAULT_SUFFIX = ""

    private Constant() {}
}
