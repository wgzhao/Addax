# InfluxDB2 Reader

InfluxDB2 Reader 插件实现了从 [InfluxDB](https://www.influxdata.com) 2.0 及以上版本读取数据。

注意，如果你的 InfluxDB 是 1.8及以下版本，则应该使用 [InfluxDBReader](../influxdbreader/) 插件

## 示例

以下示例用来演示该插件如何从指定表(即指标)上读取数据并输出到终端


### 创建 job 文件

创建 `job/influx2stream.json` 文件，内容如下：

=== "job/influx2stream.json"

  ```json
  --8<-- "jobs/influx2stream.json"
  ```

### 运行

执行下面的命令进行数据采集

```bash
bin/addax.sh job/influx2stream.json
```

## 参数说明

| 配置项   | 是否必须 | 数据类型 | 默认值 | 描述                                                                    |
| :------- | :------: | -------- | ------ | ----------------------------------------------------------------------- |
| endpoint |    是    | string   | 无     | InfluxDB 连接串 ｜                                                      |
| token    |    是    | string   | 无     | 访问数据库的 token                                                      |
| table    |    否    | list     | 无     | 所选取的需要同步的表名(即指标)                                         |
| org      |    是    | string   | 无     | 指定 InfluxDB 的 org 名称                                               |
| bucket   |    是    | string   | 无     | 指定 InfluxDB 的 bucket 名称                                            |
| column   |    否    | list     | 无     | 所配置的表中需要同步的列名集合，详细描述见 [rdbmreader][1] |
| range    |    是    | list     | 无     | 读取数据的时间范围                                                      |
| limit    |    否    | int      | 无     | 限制获取记录数                                                          |

### column

如果不指定 `column`, 或者指定 `column` 为 `["*"]` ， 则会读取所有有效的 `_field` 字段以及 `_time` 字段
否则按照指定字段读取

### range

`range` 用来指定读取指标的时间范围，其格式如下:

```json
{
  "range": ["start_time", "end_time"]
}
```

`range`  由两个字符串组成的列表组成，第一个字符串表示开始时间，第二个表示结束时间。其时间表达方式要求符合 [Flux 格式要求][2],类似这样:

```json
{
  "range": ["-15h", "-2h"]
}
```

其中第二个结束时间如果不想指定，可以不写，类似这样：

```json
{
  "range": ["-15h"]
}
```

## 类型转换

当前实现是将所有字段当作字符串处理

## 限制

1. 当前插件仅支持 2.0 及以上版本


[1]: ../rdbmsreader
[2]: https://docs.influxdata.com/influxdb/v2.0/query-data/flux/