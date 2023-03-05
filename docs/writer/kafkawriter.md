# Kafka Writer

KafkaWriter 插件实现了将数据以 json 格式写入 Kafka 的功能。 该插件在 `4.0.9` 版本中引入。

## 示例

以下配置演示了如何从内存读取数据并写入到 kafka 的指定 topic 中。

### 创建任务文件

首先创建一个任务文件  `stream2kafka.json` , 内容如下：

```json
--8<-- "jobs/stream2kafka.json"
```

### 运行

执行  `bin/addax.sh stream2kafka.json` 命令，获得类似下面的输出：

```shell
--8<-- "output/stream2kafka.txt"
```

我们使用 kafka 自带的 `kafka-console-consumer.sh` 尝试读取数据，输出如下：

```shell
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-1 --from-beginning

{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":916}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":572}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":88}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":33}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":697}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":381}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":304}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":103}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":967}
{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":147}
```

## 参数说明

| 配置项     | 是否必须 | 数据类型 | 默认值 | 描述                                                                                  |
| :--------- | :------: | -------- | ------ | ------------------------------------------------------------------------------------- |
| brokerList |    是    | string   | 无     | 连接 kafka 服务的 broker 配置，类似 `localhost:9092` ，多个 broker之间用逗号(`,`)分隔 |
| topic      |    是    | string   | 无     | 要写入的 topic                                                                        |
| batchSize  |    否    | int      | 1204   | 设置 Kafka 的 `batch.size` 参数                                                       |
| column     |    是    | list     | 无     | 所配置的表中需要同步的列名集合，不允许为 `*`                                          |
| properties |    否    | dict     | 无     | 需要设置的其他 kafka 连接参数                                                         |

## 限制

1. 仅支持 Kafka `1.0` 及以上版本，低于该版本的无法确定是否能写入
2. 当前不支持启用了 `kerberos` 认证的 kafka 服务