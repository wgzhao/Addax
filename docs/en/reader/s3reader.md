# S3 Reader

S3 Reader plugin is used to read data on Amazon AWS S3 storage. In implementation, this plugin is written based on S3's official [SDK 2.0](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html).

This plugin also supports reading storage services compatible with S3 protocol, such as [MinIO](https://min.io/).

## Configuration Example

The following sample configuration is used to read two files from S3 storage and print them out

```json
--8<-- "jobs/s3reader.json"
```

## Parameters

| Configuration          | Required | Data Type | Default Value   | Description                                                     |
|:-----------------------|:--------:|-----------|-----------------|---------------------------------------------------------------|
| endpoint               | Yes      | string    | None            | S3 Server EndPoint address, e.g. `s3.xx.amazonaws.com`       |
| region                 | Yes      | string    | None            | S3 Server Region address, e.g. `ap-southeast-1`              |
| accessId               | Yes      | string    | None            | Access ID                                                     |
| accessKey              | Yes      | string    | None            | Access Key                                                    |
| bucket                 | Yes      | string    | None            | Bucket to read                                                |
| object                 | Yes      | list      | None            | Objects to read, can specify multiple and wildcard patterns, see description below |
| column                 | Yes      | list      | None            | Column information of objects to read, refer to `column` description in [RDBMS Reader][1] |
| fieldDelimiter         | No       | string    | `,`             | Field delimiter for reading, only supports single character    |
| compress               | No       | string    | None            | File compression format, default is no compression            |
| encoding               | No       | string    | `utf8`          | File encoding format                                          |
| writeMode              | No       | string    | `nonConflict`   |                                                               |
| pathStyleAccessEnabled | No       | boolean   | false           | Whether to enable path-style access mode                      |

[1]: ../rdbmsreader

### object

When specifying a single object, the plugin can currently only use single-threaded data extraction.