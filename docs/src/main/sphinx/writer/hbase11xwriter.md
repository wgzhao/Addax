# Hbase11XWriter 插件文档

## 1 快速介绍

HbaseWriter 插件实现了从向Hbase中写取数据。在底层实现上，HbaseWriter 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 put 方式写入Hbase。

### 1.1支持功能

- 目前HbaseWriter支持源端多个字段拼接作为hbase 表的 rowkey，具体配置参考：rowkeyColumn配置；
- 写入hbase的时间戳（版本）支持：用当前时间作为版本，指定源端列作为版本，指定一个时间 三种方式作为版本；

### 1.2 限制

1. 目前只支持源端为横表写入，不支持竖表（源端读出的为四元组: `rowKey，family:qualifier，timestamp，value` )模式的数据写入；
2. 目前不支持写入hbase前清空表数据

## 2 实现原理

简而言之，HbaseWriter 通过 HBase 的 Java 客户端，通过 HTable, Put等 API，将从上游Reader读取的数据写入HBase 你hbase11xwriter与hbase094xwriter的主要不同在于API的调用不同，

## 3 功能说明

### 3.1 配置样例

配置一个从本地写入hbase1.1.x的作业：

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 5,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/tmp/normal.txt",
            "charset": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "String"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "string"
              },
              {
                "index": 3,
                "type": "string"
              },
              {
                "index": 4,
                "type": "string"
              },
              {
                "index": 5,
                "type": "string"
              },
              {
                "index": 6,
                "type": "string"
              }
            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "hbase11xwriter",
          "parameter": {
            "hbaseConfig": {
              "hbase.zookeeper.quorum": "***"
            },
            "table": "writer",
            "mode": "normal",
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
            ],
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
              },
              {
                "index": 3,
                "name": "cf1:q3",
                "type": "string"
              },
              {
                "index": 4,
                "name": "cf2:q1",
                "type": "string"
              },
              {
                "index": 5,
                "name": "cf2:q2",
                "type": "string"
              },
              {
                "index": 6,
                "name": "cf2:q3",
                "type": "string"
              }
            ],
            "versionColumn": {
              "index": -1,
              "value": "123456789"
            },
            "encoding": "utf-8"
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

| 配置项          | 是否必须 | 默认值 | 描述                                                                                                                              |
| :-------------- | :------: | ------ | --------------------------------------------------------------------------------------------------------------------------------- |
| hbaseConfig     |    是    | 无     | 连接HBase集群需要的配置信息,JSON格式, hbase.zookeeper.quorum为必填项，其他 HBase client的配置为可选项                             |
| mode            |    是    | 无     | 写乳hbase的模式，目前仅支持 `normal` 模式                                                                                         |
| table           |    是    | 无     | 要读取的 hbase 表名（大小写敏感）                                                                                                 |
| encoding        |    否    | UTF-8  | 编码方式，UTF-8 或是 GBK，用于对二进制存储的 `HBase byte[]` 转为 String 时的编码                                                  |
| column          |    是    | 无     | 要写入的hbase字段，normal 模式与multiVersionFixedColumn 模式下必填项, 详细说明见下文                                              |
| rowkeyColumn    |    是    | 无     | 要写入的hbase的rowkey列, 详细说明见下文                                                                                           |
| versionColumn   |    否    | 无     | 指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一,详见下文                                                    |
| nullMode        |    否    | skip   | 读取的null值时，如何处理, `skip` 表示不向hbase写这列；`empty`：写入 `HConstants.EMPTY_BYTE_ARRAY`，即`new byte [0]`               |
| walFlag         |    否    | false  | 是否写WAL, true表示写入, false 表示不写                                                                                           |
| writeBufferSize |    否    | 8M     | 设置HBae client的写buffer大小，单位字节                                                                                           |
| maxVersion      |    是    | 无     | 指定在多版本模式下的hbasereader读取的版本数，取值只能为－1或者大于1的数字，－1表示读取所有版本, multiVersionFixedColumn模式下必填 |
| range           |    否    | 无     | 指定hbasereader读取的rowkey范围, 详见下文                                                                                         |
| scanCacheSize   |    否    | 256    | Hbase client每次rpc从服务器端读取的行数                                                                                           |
| scanBatchSize   |    否    | 100    | Hbase client每次rpc从服务器端读取的列数                                                                                           |

#### column

要写入的hbase字段。index：指定该列对应reader端column的索引，从0开始；name：指定hbase表中的列，必须为 列族:列名 的格式；type：指定写入数据类型，用于转换HBase byte[]。配置格式如下：

```json
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
```

#### rowkeyColumn

要写入的hbase的rowkey列。index：指定该列对应reader端column的索引，从0开始，若为常量index为－1；type：指定写入数据类型，用于转换HBase byte[]；value：配置常量，常作为多个字段的拼接符。hbasewriter会将rowkeyColumn中所有列按照配置顺序进行拼接作为写入hbase的rowkey，不能全为常量。配置格式如下：

```json
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
```

#### versionColumn

指定写入hbase的时间戳。支持：当前时间、指定时间列，指定时间，三者选一。若不配置表示用当前时间。

index：指定对应reader端column的索引，从0开始，需保证能转换为long,若是Date类型， 会尝试用`yyyy-MM-dd HH:mm:ss`和`yyyy-MM-dd HH:mm:ss SSS`去解析； 若为指定时间index为 `－1`；

value：指定时间的值,long值。配置格式如下：

```json
"versionColumn":{
"index": 1
}
```

或者

```json
"versionColumn":{
"index": －1,
"value": 123456789
}
```

### 3.3 HBase支持的列类型

- BOOLEAN
- SHORT
- INT
- LONG
- FLOAT
- DOUBLE
- STRING

请注意: 除上述罗列字段类型外，其他类型均不支持
