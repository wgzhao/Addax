# 任务配置

一个采集任务就是一个 JSON 格式配置文件，该配置文件的模板如下：

```json
{
  "job": {
    "settings": {},
    "content": {
      "reader": {},
      "writer": {},
      "transformer": []
    }
  }
}
```

任务配置由 key 为 `job` 的字典组成，其字典元素由三部分组成：

- `settings`:  用来定义本次任务的一些控制参数，比如指定多少线程，最大错误率，最大错误记录条数等，这是可选配置。
- `reader`: 用来配置数据读取所需要的相关信息，这是必填内容
- `writer`: 用来配置写入数据所需要的相关信息，这是必填内容
- `transformer`: 数据转换规则，如果需要对读取的数据在写入之前做一些变换，可以配置该项，否则可以不配置

## reader 配置项

`reader` 配置项依据不同的 reader 插件而有些微不同，但大部分的配置基本相同，特别是针对关系型数据库而言，其基本配置如下：

```json
{
  "name": "mysqlreader",
  "parameter": {
    "username": "",
    "password": "",
    "column": [],
    "autoPk": false,
    "splitPk": "",
    "connection": [
      {
        "jdbcUrl": [],
        "table": []
      }
    ],
    "where": ""
  }
}
```

其中 `name` 是插件的名称，每个插件的名称都是唯一的，每个插件更详细的配置可以参考读取插件章节的各插件内容

## writer 配置项

`writer` 配置项和 `reader` 配置项差不多，其基本模板如下：

```json
{
  "name": "mysqlwriter",
  "parameter": {
    "username": "",
    "password": "",
    "writeMode": "",
    "column": [],
    "session": [],
    "preSql": [],
    "postSql": [],
    "connection": [
      {
        "jdbcUrl": "",
        "table": []
      }
    ]
  }
}
```

同样的，这里的 `name` 也是唯一的，每个插件更详细的配置可以参考写入插件章节的各插件内容

## settings 配置项

`settings` 可配置的内容如下：

```json
{
  "speed": {
    "byte": -1,
    "record": 100,
    "channel": 1
  },
  "errorLimit": {
    "record": 0,
    "percentage": 0.02
  }
}
```

解释如下：

## `speed`

顾名思义，这里是用来做流控的配置项，可以从网络传输速度，每秒的记录数以及线程数上做控制，分别描述如下：

### `speed.byte`

设置每秒可获取的字节数(Bps)，一般是为了防止执行任务时将整个带宽跑满，从而影响到其他服务。如果不做限制，可设置为 `-1`

### `speed.record`

设置记录每秒可获取的最大记录条数，该参数需要和 `speed.byte` 配合使用

### `speed.channel`

设置通道数，该通道路确定了总的 Task 线程数，假定设定 `speed.channel` 为 13， 则一共有 13 个 Task.
然后根据 `conf/core.json` 配置中的 `taskGroup.channel` 配置来确定要创建的 taskGroup 数量。即 `taskGroup = speed.channel / taskGroup.channel`.

## `errorLimit`

`errorLimit` 用来配置在数据写入报错时的行为，具体如下

### `errorLimit.record`

允许错误的记录条数，如果超过这个数，则认为本次任务失败，否则认为成功

### `errorLimit.percentage`

允许错误记录的比率，超过这个比率，则认为本次任务失败，否则认为成功

注意，上述参数在 `conf/core.json` 配置文件均有默认配置，用来控制全局的设置。
