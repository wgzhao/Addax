# MongoDBReader 插件文档

## 1 快速介绍

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。最新版本的Mongo已经将DB锁的粒度从DB级别降低到document级别，配合上MongoDB强大的索引功能，基本可以达到高性能的读取MongoDB的需求。

## 2 实现原理

MongoDBReader通过Datax框架从MongoDB并行的读取数据，通过主控的JOB程序按照指定的规则对MongoDB中的数据进行分片，并行读取，然后将MongoDB支持的类型通过逐一判断转换成Datax支持的类型。

## 3 功能说明

### 3.1 配置样例

该示例从ODPS读一份数据到MongoDB。

```json
{
"job": {
    "setting": {
        "speed": {
            "channel": 2
        }
    },
    "content": [
        {
            "reader": {
            "name": "mongodbreader",
            "parameter": {
            "address": ["127.0.0.1:27017"],
            "userName": "",
            "userPassword": "",
            "dbName": "tag_per_data",
            "collectionName": "tag_data12",
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
                "name": "odpswriter",
                "parameter": {
                    "project": "tb_ai_recommendation",
                    "table": "jianying_tag_datax_read_test01",
                    "column": [
                        "unique_id",
                        "sid",
                        "user_id",
                        "auction_id",
                        "content_type",
                        "pool_type",
                        "frontcat_id",
                        "categoryid",
                        "gmt_create",
                        "taglist",
                        "property",
                        "scorea",
                        "scoreb"
                    ],
                    "accessId": "**************",
                    "accessKey": "********************",
                    "truncate": true,
                    "odpsServer": "xxx/api",
                    "tunnelServer": "xxx",
     "accountType": "aliyun"
                }
            }
        }
    ]
}
}
```

#### 4 参数说明

| 配置项         | 是否必须 | 默认值 | 描述                                                                                 |
| :------------- | :------: | ------ | ------------------------------------------------------------------------------------ |
| address        |    是    | 无     | MongoDB的数据地址信息，因为MonogDB可能是个集群，则ip端口信息需要以Json数组的形式给出 |
| userName       |    否    | 无     | MongoDB的用户名                                                                      |
| userPassword   |    否    | 无     | MongoDB的密码                                                                        |
| collectionName |    是    | 无     | MonogoDB的集合名                                                                     |
| column         |    是    | 无     | MongoDB的文档列名                                                                    |
| name           |    是    | 无     | Column的名字                                                                         |
| type           |    否    | 无     | Column的类型                                                                         |
| splitter       |    否    | 无     | 指定 MongoDB数组转为字符串的分隔符                                                   |

#### 5 类型转换

| DataX 内部类型 | MongoDB 数据类型 |
| -------------- | ---------------- |
| Long           | int, Long        |
| Double         | double           |
| String         | string, array    |
| Date           | date             |
| Boolean        | boolean          |
| Bytes          | bytes            |
