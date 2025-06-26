/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.s3writer;

import com.wgzhao.addax.core.base.Key;

public class S3Key extends Key
{
    public static final String REGION = "region";

    public static final String ENDPOINT = "endpoint";

    public static final String ACCESS_ID = "accessId";

    public static final String ACCESS_KEY = "accessKey";

    public static final String BUCKET = "bucket";

    public static final String OBJECT = "object";

    // unit: MB
    public static final String MAX_FILE_SIZE = "maxFileSize";

    public static final String DEFAULT_SUFFIX = "defaultSuffix";

    public static final String PATH_STYLE_ACCESS_ENABLED = "pathStyleAccessEnabled";

    public static final String FILE_TYPE = "fileType";

    public static final String SSL_ENABLED = "sslEnabled";


}
