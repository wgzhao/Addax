# MongoDB Reader

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。


## 配置样例

该示例从MongoDB中读一张表并打印到终端

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "mongodbreader",
          "parameter": {
            "address": [
              "127.0.0.1:32768"
            ],
            "userName": "",
            "userPassword": "",
            "dbName": "tag_per_data",
            "collectionName": "tag_data",
            "column": [
              {
                "name": "unique_id",
                "type": "string"
              },
              {
                "name": "sid",
                "type": "string"
              },
              {
                "name": "user_id",
                "type": "string"
              },
              {
                "name": "auction_id",
                "type": "string"
              },
              {
                "name": "content_type",
                "type": "string"
              },
              {
                "name": "pool_type",
                "type": "string"
              },
              {
                "name": "frontcat_id",
                "type": "Array",
                "spliter": ""
              },
              {
                "name": "categoryid",
                "type": "Array",
                "spliter": ""
              },
              {
                "name": "gmt_create",
                "type": "string"
              },
              {
                "name": "taglist",
                "type": "Array",
                "spliter": " "
              },
              {
                "name": "property",
                "type": "string"
              },
              {
                "name": "scorea",
                "type": "int"
              },
              {
                "name": "scoreb",
                "type": "int"
              },
              {
                "name": "scorec",
                "type": "int"
              }
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": "true"
          }
        }
      }
    ]
  }
}
```

## 参数说明

| 配置项         | 是否必须 | 默认值 | 描述                                                                                 |
| :------------- | :------: | ------ | ------------------------------------------------------------------------------------ |
| address        |    是    | 无     | MongoDB的数据地址信息，因为 MonogDB 可能是个集群，则ip端口信息需要以Json数组的形式给出 |
| userName       |    否    | 无     | MongoDB的用户名                                                                      |
| userPassword   |    否    | 无     | MongoDB的密码                                                                        |
| collectionName |    是    | 无     | MongoDB的集合名                                                                     |
| column         |    是    | 无     | MongoDB的文档列名                                                                    |
| name           |    是    | 无     | Column的名字                                                                         |
| type           |    否    | 无     | Column的类型                                                                         |
| splitter       |    否    | 无     | 指定 MongoDB数组转为字符串的分隔符                                                   |

## 类型转换

| Addax 内部类型 | MongoDB 数据类型 |
| -------------- | ---------------- |
| Long           | int, Long        |
| Double         | double           |
| String         | string, array    |
| Date           | date             |
| Boolean        | boolean          |
| Bytes          | bytes            |
