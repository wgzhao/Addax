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

public class Key
{
    // must have
    public static final String FILE_NAME = "fileName";

    // must have
    public static final String WRITE_MODE = "writeMode";

    // not must , not default ,
    public static final String FIELD_DELIMITER = "fieldDelimiter";

    // not must, default UTF-8
    public static final String ENCODING = "encoding";

    // not must, default no compress
    public static final String COMPRESS = "compress";

    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";

    // not must, date format old style, do not use this
    public static final String FORMAT = "format";
    // for writers ' data format
    public static final String DATE_FORMAT = "dateFormat";

    // csv or plain text
    public static final String FILE_FORMAT = "fileFormat";

    // writer headers
    public static final String HEADER = "header";

    // writer maxFileSize
//    public static final String MAX_FILE_SIZE = "maxFileSize"

    // writer file type suffix, like .txt  .csv
    public static final String SUFFIX = "suffix";

    private Key() {}
}
