# S3 Writer

S3 Writer plugin is used to write data to Amazon AWS S3 storage, as well as S3 protocol compatible storage, such as [MinIO](https://min.io).

In implementation, this plugin is written based on S3's official [SDK 2.0](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html).

## Configuration Example

The following configuration is used to read data from memory and write to specified S3 bucket.

```json
--8<-- "jobs/s3writer.json"
```

## Parameters

| Configuration   | Required | Data Type | Default Value | Description                                                      |
|:----------------|:--------:|-----------|---------------|------------------------------------------------------------------|
| endpoint        | Yes      | string    | None          | S3 Server EndPoint address, e.g. `s3.xx.amazonaws.com`         |
| region          | Yes      | string    | None          | S3 Server Region address, e.g. `ap-southeast-1`                |
| accessId        | Yes      | string    | None          | Access ID                                                        |
| accessKey       | Yes      | string    | None          | Access Key                                                       |
| bucket          | Yes      | string    | None          | Bucket to write to                                               |
| object          | Yes      | string    | None          | Object to write to, see notes below                             |
| fieldDelimiter  | No       | char      | `','`         | Field delimiter                                                  |
| nullFormat      | No       | char      | `\N`          | What character to use when value is null                        |
| header          | No       | list      | None          | Write file header information, e.g. `["id","title","url"]`      |
| maxFileSize     | No       | int       | `100000`      | Size of single object, in MB                                    |
| encoding        | No       | string    | `utf-8`       | File encoding format                                             |