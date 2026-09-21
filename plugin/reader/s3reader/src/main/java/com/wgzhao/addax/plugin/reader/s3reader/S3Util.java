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

package com.wgzhao.addax.plugin.reader.s3reader;

import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;

import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;

/**
 * S3 Util.
 *
 * <p>The writer carries a copy of this class, since each plugin is packaged on its own. Keep the
 * two in step.
 */
public final class S3Util
{
    private S3Util()
    {
    }

    /** Inits3client. */
    public static S3Client initS3Client(Configuration conf) {
        String regionStr = conf.getString(S3Key.REGION);
        Region region = Region.of(regionStr);
        String accessId = conf.getString(S3Key.ACCESS_ID);
        String accessKey = conf.getString(S3Key.ACCESS_KEY);
        boolean pathStyleAccessEnabled = conf.getBool(S3Key.PATH_STYLE_ACCESS_ENABLED, false);
        String endpoint = conf.getString(S3Key.ENDPOINT);

        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessId, accessKey);
            S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .region(region)
                    .forcePathStyle(pathStyleAccessEnabled);
            // the endpoint is optional: with the region alone the client resolves the AWS endpoint
            // itself, an S3 compatible service has to name its own
            if (endpoint != null && !endpoint.isBlank()) {
                builder.endpointOverride(endpointUri(endpoint));
            }
            return builder.build();
        } catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, e.getMessage());
        }
    }

    /**
     * The endpoint of the service as an URI. An endpoint without a scheme cannot be reached, and
     * the client would only fail later with it, so it is rejected here.
     *
     * @param endpoint the configured endpoint
     * @return the endpoint as an URI
     */
    private static URI endpointUri(String endpoint) {
        URI uri;
        try {
            uri = new URI(endpoint);
        }
        catch (URISyntaxException e) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The endpoint [%s] is not a valid URI", endpoint), e);
        }
        if (uri.getScheme() == null) {
            throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                    String.format("The endpoint [%s] needs a scheme, for example https://%s", endpoint, endpoint));
        }
        return uri;
    }
}
