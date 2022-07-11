# S3 Reader

S3Reader 插件用于读取 Amazon AWS S3 存储上的数据。在实现上，本插件基于 S3 官方的 [SDK 2.0](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.
html) 编写。

同时本插件也支持读取兼容 S3 协议的存储服务，比如 [MinIO](https://min.io/)


## 配置样例

以下样例配置用于从 S3 存储上读取两个文件，并打印出来

```json
--8<-- "jobs/s3reader.json"
```

## 参数说明


| 配置项    | 是否必须 | 数据类型 | 默认值 | 描述                                                                        |
| :-------- | :------: | -------- | ------ | -------------------------------------------------------------------------------------------------------------------- |
| endpoint | 是   | string | 无  |S3 Server的 EndPoint地址，例如 `s3.xx.amazonaws.com` |
| region |  是   | string | 无 | S3 Server的 Region 地址，例如 `ap-southeast-1` |
| accessId |  是   | string | 无  | 访问 ID |
| accessKey |  是   | string | 无  | 访问 Key |
| bucket |  是   | string | 无  | 要读取的 bucket |
| object |  是   | list | 无 | 要读取的 object，可以填写多个以及通配符方式，详见下面说明 |
| column |  是   | list | 无 | 读取的 object 的列信息，填写方式见  [rdbmsreader](../rdbmsreader) 中 `column` 描述 |
| fieldDelimiter | 否 | string | `,` |  读取的字段分隔符，仅支持单字符 |
| compress | 否  | string | 无 | 文件压缩格式，默认不压缩 |
| encoding | 否  | string | `utf8` | 文件编码格式 |
| writeMode | 否 | string | `nonConflict` |

### object

当指定单个 object，插件暂时只能使用单线程进行数据抽取。

当指定多个 object，插件支持使用多线程进行数据抽取。线程并发数通过通道数指定。

当指定通配符，插件尝试遍历出多个 object 信息。

例如: 指定 `/*` 代表读取 bucket 下所有的 object，指定 `/foo/*` 代表读取 `foo` 目录下所有的 object。

## 类型转换

| Addax 内部类型 | S3 数据类型                                                |
| -------------- | ------------------------------------------------------------- |
| Long           | int, tinyint, smallint, mediumint, int, bigint                |
| Double         | float, double, decimal                                        |
| String         | varchar, char, tinytext, text, mediumtext, longtext, year,xml |
| Date           | date, datetime, timestamp, time                               |
| Boolean        | bit, bool                                                     |
| Bytes          | tinyblob, mediumblob, blob, longblob, varbinary               |

## 限制说明

1. 仅支持读取文本文件
