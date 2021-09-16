# MongoDB Reader

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。

## 配置样例

该示例从MongoDB中读一张表并打印到终端

=== "job/mongo2stream.json"

  ```json
  --8<-- "jobs/mongoreader.json"
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
