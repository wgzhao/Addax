package com.wgzhao.addax.plugin.reader.s3reader;

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
        boolean pathStyleAccessEnabled = conf.getBool(S3Key.PATH_STYLE_ACCESS_ENABLED, false);

        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessId, accessKey);
            return S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .region(region)
                    .endpointOverride(URI.create(conf.getString(S3Key.ENDPOINT)))
                    .forcePathStyle(pathStyleAccessEnabled)
                    .build();
        } catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(
                    ILLEGAL_VALUE, e.getMessage());
        }
    }
}
