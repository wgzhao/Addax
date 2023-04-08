# ElasticSearch Writer

elasticsearchWriter 插件用于向 [ElasticSearch](https://www.elastic.co/cn/elastic-stack/) 写入数据。其实现是通过 elasticsearch 的 rest api 接口， 批量把据写入 elasticsearch

## 配置样例

=== "job/stream2es.json"

```json
--8<-- "jobs/eswriter.json"
```

## 参数说明

| 配置项           | 是否必须 | 默认值   | 描述                                            |
| :--------------- | :------: | -------- |-----------------------------------------------|
| endpoint         |    是    | 无       | ElasticSearch 的连接地址,如果是集群，则多个地址用逗号(,)分割       |
| accessId         |    否    | 空       | http auth 中的 user, 默认为空                       |
| accessKey        |    否    | 空       | http auth 中的 password                         |
| index            |    是    | 无       | index 名                                       |
| type             |    否    | index 名 | index 的 type 名                                |
| cleanup          |    否    | false    | 是否删除原表                                        |
| batchSize        |    否    | 1000     | 每次批量数据的条数                                     |
| trySize          |    否    | 30       | 失败后重试的次数                                      |
| timeout          |    否    | 600000   | 客户端超时时间，单位为毫秒(ms)                             |
| discovery        |    否    | false    | 启用节点发现将(轮询)并定期更新客户机中的服务器列表                    |
| compression      |    否    | true     | 否是开启 http 请求压缩                                |
| multiThread      |    否    | true     | 是否开启多线程 http 请求                               |
| ignoreWriteError |    否    | false    | 写入错误时，是否重试，如果是 `true` 则表示一直重试，否则忽略该条数据        |
| ignoreParseError |    否    | true     | 解析数据格式错误时，是否继续写入                              |
| alias            |    否    | 无       | 数据导入完成后写入别名                                   |
| aliasMode        |    否    | append   | 数据导入完成后增加别名的模式，append(增加模式), exclusive(只留这一个) |
| settings         |    否    | 无       | 创建 index 时候的 settings, 与 elasticsearch 官方相同   |
| splitter         |    否    | `,`      | 如果插入数据是 array，就使用指定分隔符                        |
| column           |    是    | 无       | 字段类型，文档中给出的样例中包含了全部支持的字段类型                    |
| dynamic          |    否    | false    | 不使用 addax 的 mappings，使用 es 自己的自动 mappings     |

## 约束限制

- 如果导入 id，这样数据导入失败也会重试，重新导入也仅仅是覆盖，保证数据一致性
- 如果不导入 id，就是 append_only 模式，elasticsearch 自动生成 id，速度会提升 20%左右，但数据无法修复，适合日志型数据(对数据精度要求不高的)
