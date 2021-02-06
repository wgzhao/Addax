# MongoDBReader 插件文档

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。最新版本的Mongo已经将DB锁的粒度从DB级别降低到document级别，配合上MongoDB强大的索引功能，基本可以达到高性能的读取MongoDB的需求。

##  配置样例

该示例从MongoDB中读一张表并打印到终端

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
          "name": "mongodbreader",
          "parameter": {
            "address": [
              "127.0.0.1:32768"
            ],
            "username": "",
            "password": "",
            "dbName": "tag_per_data",
            "collection": "tag_data",
            "column": [
              "auction_id", "categoryid", "content_type", "frontcat_id", "gmt_create", "pool_type", 
              "property", "scorea", "scoreb", "scorec", "sid", "taglist", "unique_id", "user_id"
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
| address        |    是    | 无     | MongoDB的数据地址信息，因为MonogDB可能是个集群，则ip端口信息需要以Json数组的形式给出 |
| username       |    否    | 无     | MongoDB的用户名                                                                      |
| password       |    否    | 无     | MongoDB的密码                                                                        |
| collection     |    是    | 无     | MonogoDB的集合名                                                                     |
| column         |    是    | 无     | MongoDB的文档列名                                                                    | |

### column

如果需要获取表的所有字段，则可以填写 "*" 来表示，这种情况下，`_id` 字段是不获取的。
另外还需要注意的是，"*" 的情况下，插件的逻辑是获取集合的第一条记录，然后解析出字段名，如果第一条记录的字段不完整的，
则会导致后续的所有记录的字段都不完整。
因此，使用 "*" 需要谨慎，并推荐明确写出需要获取的字段。

#### 5 类型转换

| DataX 内部类型 | MongoDB 数据类型 |
| -------------- | ---------------- |
| Long           | int, Long        |
| Double         | double           |
| String         | string, array    |
| Date           | date             |
| Boolean        | boolean          |
| Bytes          | bytes            |
