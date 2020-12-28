# ClickHouseWriter 插件文档

## 1 快速介绍

ClickHouseWriter 插件实现了写入数据ClickHouse。在底层实现上，ClickHouseWriter 通过 JDBC 连接远程 ClickHouse 数据库，并执行相应的 `insert into ....` 语句将数据插入到ClickHouse库中。

## 2 实现原理

使用clickhousewriter的官方jdbc接口， 批量把从reader读入的数据写入ClickHouse

## 3 功能说明

### 3.1 配置样例

配置一个从内存读取数据并写入ClickHouse数据库的作业:

```json
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "writer": {
                    "name": "clickhousewriter",
                    "parameter": {
                        "username": "default",
                        "password": "",
                        "column": [ "col1", "col2","col3","col4" ],
                        "connection": [
                            {
                                "table": [
                                    "test_tbl"
                                ],
                                "jdbcUrl": "jdbc:clickhouse://127.0.0.1:8123/default"
                            }
                        ]
                    }
                },
               "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19890604,
                                "type": "long"
                            },
                            {
                                "value": "1989-06-04 00:00:00",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
              }
           }
        ]
    }
}
```

#### 3.2 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                    |
| :-------- | :------: | ------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | ClickHouse JDBC 连接信息 ,可按照官方规范填写连接附件控制信息。具体请参看[ClickHouse官方文档](https://github.com/yandex/clickhouse-jdbc) |
| username  |    是    | 无     | 数据源的用户名                                                                                                                          |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                                                  |
| table     |    是    | 无     | 所选取的需要同步的表 ,当配置为多张表时，用户自己需保证多张表是同一schema结构                                                            |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合, 使用JSON的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `"['*']"`                         |
| batchSize |    否    | 2048   | 每次批量数据的条数                                                                                                                      |
