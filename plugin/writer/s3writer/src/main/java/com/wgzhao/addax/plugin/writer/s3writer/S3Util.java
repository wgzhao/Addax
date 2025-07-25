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

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

public class S3Util
{
    public static S3Client initS3Client(Configuration conf) {
        String regionStr = conf.getString(S3Key.REGION);
        Region region = Region.of(regionStr);
        String accessId = conf.getString(S3Key.ACCESS_ID);
        String accessKey = conf.getString(S3Key.ACCESS_KEY);
        String pathStyleAccessEnabled = conf.getString(S3Key.PATH_STYLE_ACCESS_ENABLED, "false");

        return initS3Client(conf.getString(S3Key.ENDPOINT), region, accessId, accessKey, pathStyleAccessEnabled);

    }

    public static S3Client initS3Client(String endpoint, Region region, String accessId, String accessKey, String pathStyleAccessEnabled) {
        if (null == region) {
            region = Region.of("ap-northeast-1");
        }
        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessId, accessKey);
            return S3Client.builder().serviceConfiguration(e -> {
                        if ("true".equals(pathStyleAccessEnabled)) {
                            e.pathStyleAccessEnabled(true);
                        }
                    })
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .region(region)
                    .endpointOverride(URI.create(endpoint))
                    .build();
        } catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, e.getMessage());
        }
    }
}
