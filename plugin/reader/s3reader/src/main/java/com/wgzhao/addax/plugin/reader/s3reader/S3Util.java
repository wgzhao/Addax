package com.wgzhao.addax.plugin.reader.s3reader;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3Util
{
    public static S3Client initS3Client(Configuration conf) {
        String regionStr = conf.getString(S3Key.REGION);
        Region region = Region.of(regionStr);
        String accessId = conf.getString(S3Key.ACCESSID);
        String accessKey = conf.getString(S3Key.ACCESSKEY);

        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessId, accessKey);
            return S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .region(region)
                    .build();
        } catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(
                    S3ReaderErrorCode.ILLEGAL_VALUE, e.getMessage());
        }
    }
}
