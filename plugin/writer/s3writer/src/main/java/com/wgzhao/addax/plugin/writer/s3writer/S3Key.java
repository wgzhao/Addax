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
}
