# MongoDB Reader

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。

## 配置样例

该示例从MongoDB中读一张表并打印到终端

=== "job/mongo2stream.json"

  ```json
  --8<-- "jobs/mongoreader.json"
  ```

## 参数说明

| 配置项           | 是否必须  | 类型  | 默认值 | 描述                                                                                 |
| :------------- | :------: | ------| ------|------------------------------------------------------------------------------ |
| address        |    是    | list | 无     | MongoDB 的数据地址信息，因为 MongoDB 可能是个集群，则 IP 及端口信息需要以 JSON 数组的形式给出 |
| username       |    否    | string | 无     | MongoDB 用户名                                                                      |
| password       |    否    | string | 无     | MongoDB 密码                                                                        |
| database       |    是    | string | 无     | MongoDB 数据库
| collection     |    是    | string | 无     | MongoDB 的集合名                                                                     |
| column         |    是    | list   | 无     | MongoDB 的文档列名，不支持 `["*"]` 获取所有列方式                                       |
| query          |    否    | string |  无    | 自定义查询条件                          |
| fetchSize      |    否    | int    | 2048  |  批量获取的记录数   |

### query

`query` 是只符合 MongoDB 查询格式的 BSON 字符串，比如：

```json
{
  "query": "{amount: {$gt: 140900}, oc_date: {$gt: 20190110}}"
}
```

上述查询类似 SQL 中的 `where amount > 140900 and oc_date > 20190110`


## 类型转换

| Addax 内部类型 | MongoDB 数据类型 |
| -------------- | ---------------- |
| Long           | int, Long        |
| Double         | double           |
| String         | string, array    |
| Date           | date             |
| Boolean        | boolean          |
| Bytes          | bytes            |
