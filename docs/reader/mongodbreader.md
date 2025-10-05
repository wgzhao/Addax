# MongoDB Reader

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。

## 配置样例

该示例从MongoDB中读一张表并打印到终端

=== "job/mongo2stream.json"

  ```json
  --8<-- "jobs/mongoreader.json"
  ```

## 参数说明

| 配置项     | 是否必须 | 类型   | 默认值 | 描述                                              |
| :--------- | :------: | ------ | ------ | ------------------------------------------------- |
| address    |    是    | list   | 无     | MongoDB 的数据地址信息, 可写多个                  |
| username   |    否    | string | 无     | MongoDB 用户名                                    |
| password   |    否    | string | 无     | MongoDB 密码                                      |
| database   |    是    | string | 无     | MongoDB 数据库                                    |
| collection |    是    | string | 无     | MongoDB 的集合名                                  |
| column     |    是    | list   | 无     | MongoDB 的文档列名，不支持 `["*"]` 获取所有列方式 |
| query      |    否    | string | 无     | 自定义查询条件                                    |
| fetchSize  |    否    | int    | 2048   | 批量获取的记录数                                  |

### collection

这里的 `collection`  目前只支持单一 collection，因此设置类型为字符串，而不是其他插件常见的数组类型，这一点尤为注意。

### column

`column` 用来指定需要读取的字段名称，这里我们做了字段名称的组成两个假定：

- 不可能用单引号开头(`'`)
- 不可能全部由数字和点(`.`)  组成

基于以上假定，我们可以在简化 `column` 配置的同时，还可以指定一些常量作为补充字段，比如一般采集一张表，我们需要增加采集时间，采集源等常量，那么可以这样配置

```json
{
  "column": [
    "col1",
    "col2",
    "col3",
    "'source_mongodb'",
    "20211026",
    "123.12"
  ]
}
```

上述配置的后三个字段就是常量，分别当作字符类型，整型和浮点型处理。

如果字段是嵌入式的，可以用点(`.`)来表示层级关系，比如：

```json
{
  "column": [
    "col1",
    "col2",
    "col3.subcol1",
    "col3.subcol2"
  ]
}
```

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
