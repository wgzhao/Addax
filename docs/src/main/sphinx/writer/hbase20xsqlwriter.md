# HBase20xsqlwriter 插件文档

## 1. 快速介绍

HBase20xsqlwriter实现了向hbase中的SQL表(phoenix)批量导入数据的功能。Phoenix因为对rowkey做了数据编码，所以，直接使用HBaseAPI进行写入会面临手工数据转换的问题，麻烦且易错。本插件提供了SQL方式直接向Phoenix表写入数据。

在底层实现上，通过Phoenix QueryServer的轻客户端驱动，执行UPSERT语句向Phoenix写入数据。

### 1.1 支持的功能

支持带索引的表的数据导入，可以同步更新所有的索引表

### 1.2 限制

1. 要求版本为Phoenix5.x及HBase2.x
2. 仅支持通过Phoenix QeuryServer导入数据，因此您Phoenix必须启动QueryServer服务才能使用本插件
3. 不支持清空已有表数据
4. 仅支持通过phoenix创建的表，不支持原生HBase表
5. 不支持带时间戳的数据导入

## 2. 实现原理

通过Phoenix轻客户端，连接Phoenix QueryServer服务，执行UPSERT语句向表中批量写入数据。因为使用上层接口，所以，可以同步更新索引表。

## 3. 配置说明

### 3.1 配置样例

```json
{
  "job": {
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
              }
            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "hbase20xsqlwriter",
          "parameter": {
            "batchSize": "100",
            "column": [
              "UID",
              "TS",
              "EVENTID",
              "CONTENT"
            ],
            "queryServerAddress": "http://127.0.0.1:8765",
            "nullMode": "skip",
            "table": "TEST_TBL"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 5,
        "bytes": -1
      }
    }
  }
}
```

### 3.2 参数说明

| 配置项             | 是否必须 | 默认值   | 描述                                                                                          |
| :----------------- | :------: | -------- | --------------------------------------------------------------------------------------- |
| queryServerAddress |    是    | 无       | Phoenix QueryServer 地址, 该插件通过 PQS 进行连接                                             |
| serialization      |    否    | PROTOBUF | QueryServer使用的序列化协议                                                                   |
| table              |    是    | 无       | 所要读取表名                                                                                  |
| schema             |    否    | 无       | 表所在的schema                                                                                |
| batchSize          | 否  | 256 | 一次批量写入的最大行数 |
| column             |    否    | 全部列   | 列名，大小写敏感，通常phoenix的列名都是**大写**, 数据类型无需填写,会自动获取列          |
| nullMode        |    否    | skip   | 读取的null值时，如何处理, `skip` 表示不向hbase写这列；`empty`：写入 `HConstants.EMPTY_BYTE_ARRAY`，即`new byte [0]`               |
