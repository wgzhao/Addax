# HBase20 SQL Reader

HBase20 SQL Reader 插件实现了从 [Phoenix(HBase SQL)](https://phoenix.apache.org) 读取数据，对应版本为 HBase2.X 和 Phoenix5.X。

## 配置样例

配置一个从 Phoenix 同步抽取数据到本地的作业:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hbase20xsqlreader",
          "parameter": {
            "queryServerAddress": "http://127.0.0.1:8765",
            "serialization": "PROTOBUF",
            "table": "TEST",
            "column": ["ID", "NAME"],
            "splitKey": "ID"
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3,
        "bytes": -1
      }
    }
  }
}
```

## 参数说明

| 配置项             | 是否必须 | 数据类型 | 默认值   | 描述                                                                                          |
| :----------------- | :------: | -------- | -------- | --------------------------------------------------------------------------------------------- |
| queryServerAddress |    是    | string   | 无       | Phoenix QueryServer 地址, 该插件通过 PQS 进行连接                                             |
| serialization      |    否    | string   | PROTOBUF | QueryServer 使用的序列化协议                                                                  |
| table              |    是    | string   | 无       | 所要读取表名                                                                                  |
| schema             |    否    | string   | 无       | 表所在的 schema                                                                               |
| column             |    否    | list     | ``       | 填写需要从 phoenix 表中读取的列名集合，空值表示读取所有列                                     |
| splitKey           |    是    | string   | 无       | 根据数据特征动态指定切分点，对表数据按照指定的列的最大、最小值进行切分,仅支持整型和字符串类型 |
| splitPoints        |    否    | string   | 无       | 按照表的 split 进行切分                                                                       |
| where              |    否    | string   | 无       | 支持对表查询增加过滤条件，每个切分都会携带该过滤条件                                          |
| querySql           |    否    | string   | 无       | 支持指定多个查询语句，但查询列类型和数目必须保持一致                                          |

## 类型转换

目前支持大部分 Phoenix 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

| Addax 内部类型 | Phoenix 数据类型                   |
| -------------- | ---------------------------------- |
| String         | CHAR, VARCHAR                      |
| Bytes          | BINARY, VARBINARY                  |
| Bool           | BOOLEAN                            |
| Long           | INTEGER, TINYINT, SMALLINT, BIGINT |
| Double         | FLOAT, DECIMAL, DOUBLE,            |
| Date           | DATE, TIME, TIMESTAMP              |

## 约束限制

- 切分表时切分列仅支持单个列，且该列必须是表主键
- 不设置 `splitPoint` 默认使用自动切分，此时切分列仅支持整形和字符型
- 表名和 `SCHEMA` 名及列名大小写敏感，请与 Phoenix 表实际大小写保持一致
- 仅支持通过 Phoenix QueryServer 读取数据，因此您的 Phoenix 必须启动 QueryServer 服务才能使用本插件
