# Kafka Reader

Kafka Reader 插件实现从 Kafka 队列中读取 JSON 格式消息的功能。 该插件在 `4.0.10` 版本中引入。

## 示例

以下配置演示了如何从从 kafka 的读取指定 topic 中，并输出到终端上。

### 创建任务文件

首先创建一个任务文件  `kafka2stream.json` , 内容如下：

```json
--8<-- "jobs/kafka2stream.json"
```

### 运行

执行  `bin/addax.sh kafka2stream.json` 命令。


## 参数说明

| 配置项          | 是否必须 | 数据类型 | 默认值 | 描述                               |
| :-------------- | :------: | -------- | ------ | ---------------------------------- |
| brokerList      |    是    | string   | 无     | 连接 kafka 服务的 broker 配置，类似 `localhost:9092` ，多个 broker之间用逗号(`,`)分隔  |
| topic           |    是    | string   | 无     | 要写入的 topic                 |
| column          |    是    | list     | 无     | 所配置的表中需要同步的列名集合，以下详述    |
| missingKeyValue |    否    | string   | 无     | 字段不存在时用什么值填充，以下详述  |
| properties      |    否    | map     | 无     | 需要设置的其他 kafka 连接参数 |

### column

`column` 用来指定要读取的 JSON 消息中的 key，如果填写为 `*` ，则表示读取消息中的所有 key。但要注意，这种情况下，一是输出不会有排序，也就是说第每条记录的 key 的
输出顺序不确保一致。

也可以指定 key 来进行读取，比如

```json
{
  "column": ["col1", "col2", "col3"]
}
```

这样，插件会尝试按照给定的顺序去读取相应的 key，如果一条消息中要读取的 key 不存在，插件会报错并退出。如果希望不退出，则可以设置 `missingKeyValue`
他表示当要读取的 key 不存在时，用该配置的值来填充。

另外，读取的 key 的值的类型，插件会自动去猜测，如果类型无法猜测，则会当作 String 类型。

## 限制

1. 仅支持 Kafka `1.0` 及以上版本，低于该版本的无法确定是否能写入
2. 当前不支持启用了 `kerberos` 认证的 kafka 服务
