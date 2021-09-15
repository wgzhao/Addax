# HBase20xsql Writer

HBase20xsqlwriter 插件利用 Phoenix 向 HBase 2.x 写入数据。

如果 HBase 是 1.X 版本，则可以使用 [HBase11xsqlWriter](hbase11xsqlwriter) 或[HBase11xWriter](hbase11xwriter) 插件

## 配置样例

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

## 参数说明

| 配置项             | 是否必须 | 默认值   | 描述                                                                                          |
| :----------------- | :------: | -------- | --------------------------------------------------------------------------------------- |
| queryServerAddress |    是    | 无       | Phoenix QueryServer 地址, 该插件通过 PQS 进行连接                                             |
| serialization      |    否    | PROTOBUF | QueryServer使用的序列化协议                                                                   |
| table              |    是    | 无       | 所要读取表名                                                                                  |
| schema             |    否    | 无       | 表所在的schema                                                                                |
| batchSize          | 否  | 256 | 一次批量写入的最大行数 |
| column             |    否    | 全部列   | 列名，大小写敏感，通常phoenix的列名都是**大写**, 数据类型无需填写,会自动获取列          |
| nullMode        |    否    | skip   | 读取的null值时，如何处理, `skip` 表示不向hbase写这列；`empty`：写入 `HConstants.EMPTY_BYTE_ARRAY`，即`new byte [0]`               |
