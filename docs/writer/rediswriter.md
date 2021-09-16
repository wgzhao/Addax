# Redis Writer

RedisWrite 提供了还原Redis dump命令的能力，并写入到目标Redis。支持redis cluster集群、proxy、以及单机

## 配置样例

```json
--8<-- "jobs/rediswriter.json"
```

## 参数说明

| 配置项       | 是否必须 | 默认值 | 描述                                                                                                     |
| :----------- | :------: | ------ | -------------------------------------------------------------------------------------------------------- |
| uri          |    是    | 否     | redis链接,,如果是集群,单机/proxy/redis cluster集群只需填写一个地址即可, 程序会自动获取集群里的所有master |
| redisCluster |    否    | false  | redis cluster集群请务必填写此项，否者无法定位slot槽。如果是proxy或单机忽略该项                           |
| flushDB      |    否    | false  | 迁移前格式化目标Redis                                                                                    |
| batchSize    |    否    | 1000   | 每次批量处理数量。如果key过大/小,可以相应的调整                                                          |
| timeout      |    否    | 60000  | 每次执行最大超时时间, 单位毫秒(ms)                                                                       |
| include      |    否    | 无     | 要包含的 key, 支持正则表达式                                                                             |
| exclude      |    否    | 无     | 要排除的 key,支持正则表达式                                                                              |
