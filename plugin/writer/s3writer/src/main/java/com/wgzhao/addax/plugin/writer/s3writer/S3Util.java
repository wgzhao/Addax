package com.wgzhao.addax.plugin.writer.s3writer;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.util.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;

public class S3Util
{
    public static S3Client initS3Client(Configuration conf) {
        String regionStr = conf.getString(S3Key.REGION);
        Region region = Region.of(regionStr);
        String accessId = conf.getString(S3Key.ACCESS_ID);
        String accessKey = conf.getString(S3Key.ACCESS_KEY);
        String pathStyleAccessEnabled =conf.getString(S3Key.PATH_STYLE_ACCESS_ENABLED,"");

        return initS3Client(conf.getString(S3Key.ENDPOINT), region, accessId, accessKey ,pathStyleAccessEnabled);

    }

    public static S3Client initS3Client(String endpoint, Region region, String accessId, String accessKey ,String pathStyleAccessEnabled) {
        if (null == region) {
            region = Region.of("ap-northeast-1");
        }
        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessId, accessKey);
            return S3Client.builder().serviceConfiguration(e -> {
                        if("true".equals(pathStyleAccessEnabled)) {
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
