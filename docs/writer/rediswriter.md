# Redis Writer

Redis Writer 提供了还原 Redis dump 命令的能力，并写入到目标 Redis。支持 redis cluster 集群、proxy、以及单机

## 配置样例

```json
--8<-- "jobs/rediswriter.json"
```

## 参数说明

| 配置项       | 是否必须 | 数据类型 | 默认值 | 描述                                                |
| :----------- | :------: | -------- | ------ | --------------------------------------------------- |
| uri          |    是    | string   | 否     | redis链接                                           |
| redisCluster |    否    | boolean  | false  | 是否为redis cluster集群,如果是 proxy 或单机忽略该项 |
| flushDB      |    否    | boolean  | false  | 迁移前是否清空目标 Redis                            |
| batchSize    |    否    | string   | 1000   | 每次批量处理数量。如果key过大/小,可以相应的调整     |
| timeout      |    否    | string   | 60000  | 每次执行最大超时时间, 单位毫秒(ms)                  |
