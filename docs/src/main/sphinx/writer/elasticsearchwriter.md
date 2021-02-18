# ElasticSearchWriter 插件文档

## 1 快速介绍

数据导入elasticsearch的插件

## 2 实现原理

使用 elasticsearch 的rest api接口， 批量把从reader读入的数据写入elasticsearch

## 3 功能说明

### 3.1 配置样例

#### job.json

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "random": "10,1000",
                "type": "long"
              },
              {
                "value": "1.1.1.1",
                "type": "string"
              },
              {
                "value": 19890604.0,
                "type": "double"
              },
              {
                "value": 19890604,
                "type": "long"
              },
              {
                "value": 19890604,
                "type": "long"
              },
              {
                "value": "hello world",
                "type": "string"
              },
              {
                "value": "long text",
                "type": "string"
              },
              {
                "value": "41.12,-71.34",
                "type": "string"
              },
              {
                "value": "2017-05-25 11:22:33",
                "type": "string"
              }
            ],
            "sliceRecordCount": 100
          }
        },
        "writer": {
          "name": "elasticsearchwriter",
          "parameter": {
            "endpoint": "http://localhost:9200",
            "index": "test-1",
            "type": "default",
            "cleanup": true,
            "settings": {
              "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
              }
            },
            "discovery": false,
            "batchSize": 1000,
            "splitter": ",",
            "column": [
              {
                "name": "pk",
                "type": "id"
              },
              {
                "name": "col_ip",
                "type": "ip"
              },
              {
                "name": "col_double",
                "type": "double"
              },
              {
                "name": "col_long",
                "type": "long"
              },
              {
                "name": "col_integer",
                "type": "integer"
              },
              {
                "name": "col_keyword",
                "type": "keyword"
              },
              {
                "name": "col_text",
                "type": "text",
                "analyzer": "ik_max_word"
              },
              {
                "name": "col_geo_point",
                "type": "geo_point"
              },
              {
                "name": "col_date",
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
              },
              {
                "name": "col_nested1",
                "type": "nested"
              },
              {
                "name": "col_nested2",
                "type": "nested"
              },
              {
                "name": "col_object1",
                "type": "object"
              },
              {
                "name": "col_object2",
                "type": "object"
              },
              {
                "name": "col_integer_array",
                "type": "integer",
                "array": true
              },
              {
                "name": "col_geo_shape",
                "type": "geo_shape",
                "tree": "quadtree",
                "precision": "10m"
              }
            ]
          }
        }
      }
    ]
  }
}
```

#### 3.2 参数说明

| 配置项           | 是否必须 | 默认值  | 描述                                                                      |
| :--------------- | :------: | ------- | ------------------------------------------------------------------------- |
| endpoint         |    是    | 无      | ElasticSearch的连接地址                                                   |
| accessId         |    否    | 空      | http auth中的user, 默认为空                                               |
| accessKey        |    否    | 空      | http auth中的password                                                     |
| index            |    是    | 无      | elasticsearch中的index名                                                  |
| type             |    否    | index名 | lasticsearch中index的type名                                               |
| cleanup          |    否    | false   | 是否删除原表                                                              |
| batchSize        |    否    | 1000    | 每次批量数据的条数                                                        |
| trySize          |    否    | 30      | 失败后重试的次数                                                          |
| timeout          |    否    | 600000  | 客户端超时时间，单位为毫秒(ms)                                            |
| discovery        |    否    | false   | 启用节点发现将(轮询)并定期更新客户机中的服务器列表                        |
| compression      |    否    | true    | 否是开启http请求压缩                                                      |
| multiThread      |    否    | true    | 是否开启多线程http请求 ｜                                                 |
| ignoreWriteError |    否    | false   | 写入错误时，是否重试，如果是 `true` 则表示一直重试，否则忽略该条数据      |
| ignoreParseError |    否    | true    | 解析数据格式错误时，是否继续写入                                          |
| alias            |    否    | 无      | 数据导入完成后写入别名                                                    |
| aliasMode        |    否    | append  | 数据导入完成后增加别名的模式，append(增加模式), exclusive(只留这一个)     |
| settings         |    否    | 无      | 创建index时候的settings, 与elasticsearch官方相同                          |
| splitter         |    否    | `,`     | 如果插入数据是array，就使用指定分隔符                                     |
| column           |    是    | 无      | elasticsearch所支持的字段类型，文档中给出的样例中包含了全部支持的字段类型 |
| dynamic          |    否    | false   | 不使用datax的mappings，使用es自己的自动mappings                           |

## 4 约束限制

- 如果导入id，这样数据导入失败也会重试，重新导入也仅仅是覆盖，保证数据一致性
- 如果不导入id，就是append_only模式，elasticsearch自动生成id，速度会提升20%左右，但数据无法修复，适合日志型数据(对数据精度要求不高的)