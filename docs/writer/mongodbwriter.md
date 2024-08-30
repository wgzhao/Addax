# MongoDB Writer

MongoDB Writer 插件用于向 [MongoDB](https://mongodb.com) 写入数据。

## 配置样例

该示例将流式数据写入到 MongoDB 表中

=== "job/stream2mongo.json"

```json
--8<-- "jobs/mongowriter.json"
```

## 参数说明

| 配置项     | 是否必须 | 类型        | 默认值 | 描述                                                  |
| :--------- | :------: | ----------- | ------ | ----------------------------------------------------- |
| address    |    是    | list        | 无     | MongoDB 的数据地址信息                                |
| username   |    否    | string      | 无     | MongoDB 的用户名                                      |
| password   |    否    | string      | 无     | MongoDB 的密码                                        |
| collection |    是    | string      | 无     | MongoDB 的集合名                                      |
| column     |    是    | `list<map>` | 无     | MongoDB 的文档列名                                    |
| splitter   |    否    | string      | 无     | 特殊分隔符，详见下文                                  |
| writeMode  |    否    | string      | insert | 指定了传输数据时更新的信息,支持 insert， update 两种  |
| batchSize  |    否    | int         | 2048   | 指定批次输入的数量                                    |
| isUpsert   |    否    | boolean     | 无     | 当设置为 true 时，表示针对相同的 upsertKey 做更新操作 |
| upsertKey  |    否    | string      | 无     | upsertKey 指定了没行记录的业务主键。用来做更新时使用  |

### column

`column` 指定 mongo collection 的字段以及类型，如果是数组类型，还需要指定接收到的数据按照什么分割，一个 `column` 字段至少需要指定 `name` 以及 `type`，比如

```json
{
  "column": [
    {
      "name": "user_id",
      "type": "string"
    }
  ]
}
```

如果是数组类型，则需要配置 `splitter` 来告知分隔符，类似如下：

```json
{
  "column": {
    "name": "taglist",
    "type": "Array",
    "splitter": " "
  }
}
```

### splitter

当且仅当要处理的字符串要用分隔符分隔为字符数组时，才使用这个参数，通过这个参数指定的分隔符，将字符串分隔存储到 MongoDB 的数组中

### writeMode

不配置的情况下，默认采取直接插入记录的方式，如果希望实现插入更新（即记录存在则更新否则插入），可以指定为 `update` 模式，该模式下，必须同时更新的字段是哪个，比如：

```json
{
  "writeMode": "update(unique_id)"
}
```

上述配置表示依据字段 `unique_id` 来决定当前记录是插入还是更新，当前暂不支持指定多个字段

## 类型转换

| Addax 内部类型 | MongoDB 数据类型 |
| -------------- | ---------------- |
| Long           | int, Long        |
| Double         | double           |
| String         | string, array    |
| Date           | date             |
| Boolean        | boolean          |
| Bytes          | bytes            |
