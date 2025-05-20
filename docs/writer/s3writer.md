# S3 Writer

S3 Writer 插件用于将数据写入 Amazon AWS S3 存储，以及兼容 S3 协议的存储，比如 [MinIO](https://min.io)。

在实现上，本插件基于 S3 官方的 [SDK 2.0](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.
html) 编写。

## 配置样例

下面的配置用于从内存读取数据，并写入到指定的 S3 bucket 上。

```json
--8<-- "jobs/s3writer.json"
```

## 参数说明

| 配置项                    | 是否必须 | 数据类型   | 默认值                      | 描述                                             |
|:-----------------------| :------: |--------|--------------------------|------------------------------------------------|
| endpoint               |    是    | string | 无                        | S3 Server的 EndPoint地址，例如 `s3.xx.amazonaws.com` |
| region                 |    是    | string | 无                        | S3 Server的 Region 地址，例如 `ap-southeast-1`       |
| accessId               |    是    | string | 无                        | 访问 ID                                          |
| accessKey              |    是    | string | 无                        | 访问 Key                                         |
| bucket                 |    是    | string | 无                        | 要写入的 bucket                                    |
| object                 |    是    | string | 无                        | 要写入的 object，注意事项见下                             |
| fieldDelimiter         |    否    | char   | `','`                    | 字段的分隔符                                         |
| nullFormat             |    否    | char   | `\N`                     | 当值为空时，用什么字符表示                                  |
| header                 |    否    | list   | 无                        | 写入文件头信息，比如 `["id","title","url"]`              |
| maxFileSize            |    否    | int    | `100000`                 | 单个 object 的大小，单位为 MB                           |
| encoding               |    否    | string | `utf-8`                  | 文件编码格式                                         |
| writeMode              |    否    | string | `append`                 | 写入模式，详见 [hdfswriter](../hdfswriter) 中相关描述      |
| pathStyleAccessEnabled |    否    | bool   | false                    | 是否使用path access方式访问                            |
| sslEnabled             |    否    | bool   | true                     | 是否使用ssl方式访问                                    |
| fileType               |    否    | string   | `text`                   | 文件类型 text, orc ,parquet                        |
| compress              |    否    | string   | orc 默认`NONE` parquet 默认 `UNCOMPRESSED` | orc或parquet文件的压缩方式,默认不压缩                       |
### object

上述配置中的 `object` 配置的虽然是 `upload.csv` 文件，实际上在 S3 写入的文件名会在指定的文件名后面加上 `uuid` 后缀，
类似 `upload.csv_c0d2ca7df0444933a6f18ea76718b569`。 这是用于在多通道写入的情况下，确保文件名不会重名。


## 类型转换

| Addax 内部类型 | S3 数据类型                                                |
| -------------- | ------------------------------------------------------------- |
| Long           | int, tinyint, smallint, mediumint, int, bigint                |
| Double         | float, double, decimal                                        |
| String         | varchar, char, tinytext, text, mediumtext, longtext, year,xml |
| Date           | date, datetime, timestamp, time                               |
| Boolean        | bit, bool                                                     |
| Bytes          | tinyblob, mediumblob, blob, longblob, varbinary               |


