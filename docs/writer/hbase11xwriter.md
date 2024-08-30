# HBase11X Writer

Hbase11X Writer 插件实现了从向 HBase 写数据的能力。在底层实现上，HBase11X Writer 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 put 方式写入Hbase。

如果 HBase 是 2.X 版本，则需要使用 [HBase20xsqlwriter](../hbase20xsqlwriter) 插件

## 配置样例

配置一个从本地写入hbase1.1.x的作业：

```json
--8<-- "jobs/hbase11xwriter.json"
```

## 参数说明

| 配置项          | 是否必须 | 数据类型    | 默认值    | 描述                                                                                        |
| :-------------- | :------: | ----------- | --------- | ------------------------------------------------------------------------------------------- |
| hbaseConfig     |    是    | map         | 无        | 连接  HBase 集群需要的配置信息,详见示例配置                                                 |
| mode            |    是    | string      | 无        | 写入 HBase 的模式，目前仅支持 `normal` 模式                                                 |
| table           |    是    | string      | 无        | HBase 表名（大小写敏感）                                                                    |
| encoding        |    否    | string      | UTF-8     | 编码方式，`UTF-8` 或是 `GBK`，用于对二进制存储的 `HBase byte[]` 转为 String 时的编码        |
| column          |    是    | `list<map>` | 无        | 要写入的字段，`normal` 模式与  `multiVersionFixedColumn` 模式下必填项, 详细说明见下文       |
| rowkeyColumn    |    是    | `list<map>` | 无        | 要写入的 `rowkey` 列, 详细说明见下文                                                        |
| versionColumn   |    否    | string      | 无        | 指定写入的时间戳,详见下文                                                                   |
| nullMode        |    否    | string      | skip      | 读取的null值时，如何处理,                                                                   |
| walFlag         |    否    | boolean     | false     | 是否写 `WAL`, `true` 表示写入, `false` 表示不写                                             |
| writeBufferSize |    否    | int         | `8388608` | 设置写 `buffer` 大小，单位字节, 默认8M                                                      |
| maxVersion      |    是    | string      | 无        | 指定在多版本模式下读取的版本数，`-1` 表示读取所有版本, `multiVersionFixedColumn` 模式下必填 |
| range           |    否    | string      | 无        | 指定读取的 `rowkey` 范围, 详见下文                                                          |
| scanCacheSize   |    否    | string      | 256       | 每次从服务器端读取的行数                                                                    |
| scanBatchSize   |    否    | string      | 100       | 每次从服务器端读取的列数                                                                    |

### column

要写入的hbase字段。

- index：指定该列对应 reader 端 column 的索引，从 0 开始
- name：指定 hbase 表中的列，必须为 `列族:列名` 的格式
- type：指定写入数据类型，用于转换 `HBase byte[]`。

配置格式如下：

```json
{
  "column": [
    {
      "index": 1,
      "name": "cf1:q1",
      "type": "string"
    },
    {
      "index": 2,
      "name": "cf1:q2",
      "type": "string"
    }
  ]
}
```

### rowkey

要写入的 `rowkey` 列。

- index：指定该列对应 reader 端 column 的索引，从 0 开始，若为常量 index 为 －1；
- type：指定写入数据类型，用于转换 `HBase byte[]`；
- value：配置常量，常作为多个字段的拼接符。

插件会将 rowkeyColumn 中所有列按照配置顺序进行拼接作为写入 hbase 的 rowkey，不能全为常量。配置格式如下：

```json
{
  "rowkeyColumn": [
    {
      "index": 0,
      "type": "string"
    },
    {
      "index": -1,
      "type": "string",
      "value": "_"
    }
  ]
}
```

### versionColumn

指定写入的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。

index：指定对应 reader 端 column 的索引，从 0 开始，需保证能转换为 long,若是 Date 类型， 会尝试用 `yyyy-MM-dd HH:mm:ss` 和 `yyyy-MM-dd HH:mm:ss SSS` 去解析； 
若为指定时间 index 为 `－1`，则 value 为指定时间的值。配置格式如下：

```json
{
  "versionColumn": {
    "index": 1
  }
}
```

或者

```json
{
  "versionColumn": {
    "index": -1,
    "value": 123456789
  }
}
```

### nullMode

 `skip` 表示不向hbase写这列；`empty`：写入 `HConstants.EMPTY_BYTE_ARRAY`，即`new byte [0]`

## 支持的列类型

- BOOLEAN
- SHORT
- INT
- LONG
- FLOAT
- DOUBLE
- STRING

请注意: 除上述罗列字段类型外，其他类型均不支持
