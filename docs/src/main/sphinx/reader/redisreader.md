# Redis Reader

RedisReader 插件用于读取 [Redis RDB](https://redis.io) 数据

## 配置样例

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "redisreader",
          "parameter": {
            "connection": [
              {
                "uri": "file:///root/dump.rdb",
                "uri": "http://localhost/dump.rdb",
                "uri": "tcp://127.0.0.1:7001",
                "uri": "tcp://127.0.0.1:7002",
                "uri": "tcp://127.0.0.1:7003",
                "auth": "password"
              }
            ],
            "include": [
              "^user"
            ],
            "exclude": [
              "^password"
            ],
            "db": [
              0,
              1
            ]
          }
        },
        "writer": {
          "name": "rediswriter",
          "parameter": {
            "connection": [
              {
                "uri": "tcp://127.0.0.1:6379",
                "auth": "123456"
              }
            ],
            "timeout": 60000
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

## 参数说明

| 配置项          | 是否必须 | 默认值 | 描述      |
| :-------------- | :------: | ------ | ------- |
| uri | 是 | 否 | redis链接,支持多个本地rdb文件/网络rdb文件,如果是集群,填写所有master节点地址 |
| db | 否 | 无 | 需要读取的db索引,若不填写,则读取所有db |
| include | 否 | 无 | 要包含的 key, 支持正则表达式 |
| exclude | 否  | 无 | 要排除的 key,支持正则表达式 |

## 约束限制

1. 不支持直接读取任何不支持 `sync` 命令的 redis server，如果需要请备份的rdb文件进行读取。
2. 如果是原生redis cluster集群，请填写所有master节点的tcp地址，`redisreader` 插件会自动dump 所有节点的rdb文件。
3. 仅解析 `String` 数据类型，其他复合类型(`Sets`, `List` 等会忽略)
