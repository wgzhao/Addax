# MongoDB Writer

MongoDBWriter 插件用于向 [MongoDB](https://mongodb.com) 写入数据。

## 配置样例

该示例将流式数据写入到 MongoDB 表中

=== "job/stream2mongo.json"

  ```json
  --8<-- "jobs/mongowriter.json"
  ```

## 参数说明

| 配置项         | 是否必须 | 默认值 | 描述                                                                                                                  |
| :------------- | :------: | ------ | ------------------------------------------------------------------------------------------------------------------ |
| address        |    是    | 无     | MongoDB的数据地址信息，因为MongoDB可能是个集群，则ip端口信息需要以Json数组的形式给出                                           |
| userName       |    否    | 无     | MongoDB的用户名                                                                                                         |
| userPassword   |    否    | 无     | MongoDB的密码                                                                                                            |
| collectionName |    是    | 无     | MongoDB的集合名                                                                                                        |
| column         |    是    | 无     | MongoDB的文档列名                                                                                                      |
| name           |    是    | 无     | Column的名字                                                                                                          |
| type           |    否    | 无     | Column的类型                                                                                                          |
| splitter       |    否    | 无     | 特殊分隔符，当且仅当要处理的字符串要用分隔符分隔为字符数组时，才使用这个参数，通过这个参数指定的分隔符，将字符串分隔存储到MongoDB的数组中 |
| upsertInfo     |    否    | 无     | 指定了传输数据时更新的信息                                                                                              |
| isUpsert       |    否    | 无     | 当设置为true时，表示针对相同的upsertKey做更新操作                                                                         |
| upsertKey      |    否    | 无     | upsertKey指定了没行记录的业务主键。用来做更新时使用                                                                       |

## 类型转换

| Addax 内部类型 | MongoDB 数据类型 |
| -------------- | ---------------- |
| Long           | int, Long        |
| Double         | double           |
| String         | string, array    |
| Date           | date             |
| Boolean        | boolean          |
| Bytes          | bytes            |
