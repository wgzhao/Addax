# InfluxDB2 Writer

InfluxDB2Writer 插件实现了将数据写入 [InfluxDB](https://www.influxdata.com) 2.0 及以上版本的数据库的功能。

注意，如果你的 InfluxDB 是 1.8 及以下版本，则应该使用 [InfluxDBWriter](../influxdbwriter/) 插件

## 示例

以下示例用来演示该插件从内存读取数据并写入到指定表

### 创建 job 文件

创建 `job/stream2influx2.json` 文件，内容如下：

=== "job/stream2influx2.json"

  ```json
  --8<-- "jobs/stream2influx2.json"
  ```

### 运行

执行下面的命令进行数据采集

```bash
bin/addax.sh job/stream2influx2.json
```

## 参数说明

| 配置项          | 是否必须 | 数据类型 | 默认值 | 描述                               |
| :-------------- | :------: | -------- | ------ | ---------------------------------- |
| endpoint        |    是    | string   | 无     | InfluxDB 连接串                    |
| table           |    是    | string   | 无     | 要写入的表（指标）                 |
| org             |    是    | string   | 无     | 指定 InfluxDB 的 org 名称 |
| bucket          |    是    | string   | 无     | 指定 InfluxDB 的 bucket 名称 |
| token           |    是    | string   | 无     | 访问数据库的 token |
| column          |    是    | list     | 无     | 所配置的表中需要同步的列名集合     |
| tag             |    否    | list     | 无     | 要指定的 tag    |
| interval        |    否    | string   | ms     | 指定时间间隔，可以指定 `s`,`ms`,`us`, `ns` |
| batchSize       |    否    | int      | 1024   | 批量写入的大小       |

### column

InfluxDB 作为时序数据库，需要每条记录都有时间戳字段，因此会把每条的记录的第一个字段当作时间戳来处理。
`column` 只需要指定除了第一个字段外的其他字段。
比如示例中，`streamreader` 设置了4个字段，但在 `influxdb2writer` 中的 `column` 只指定了三个字段，就是因为第一个字段已经默认作为时间戳了。

### tag

用于指定指标（这里当作表）的 标签，每个 tag 使用 map 方式指定，比如示例中：

```json
{
  "tag": [
    {
      "location": "east"
    },
    {
      "lat": 23.123445
    }
  ]
}
```

map中的 key 表示标签的名称，value 表示标签值


### interval

设置时间戳的间隔频率，该字段的定义来源于 [influxdb-client-java][1] 中的 [WritePrecision.java][2]  其字符串表达的含义分别为：

- s : 秒
- ms : 毫秒
- us : 微秒
- ns : 纳秒

## 类型转换

当前支持 InfluxDB 2.0 的基本类型

[1]: https://github.com/influxdata/influxdb-client-java 
[2]: https://github.com/influxdata/influxdb-client-java/blob/master/client/src/generated/java/com/influxdb/client/domain/WritePrecision.java