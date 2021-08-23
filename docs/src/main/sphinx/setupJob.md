# 任务配置

一个采集任务就是一个 JSON 格式配置文件，该配置文件的模板如下：

```json
{
  "job": {
    "settings": {}
    "content": [
      {
        "reader": {},
        "writer": {}
        "transformer": []
      }
    ]
  }
}
```

任务配置由 key 为 `job` 的字典组成，其字典元素由三部分组成：

- `reader`: 用来配置数据读取所需要的相关信息，这是必填内容
- `writer`: 用来配置写入数据所需要的相关信息，这是必填内容
- `transformer`: 数据转换规则，如果需要对读取的数据在写入之前做一些变换，可以配置该项，否则可以不配置。

## reader 配置项

`reader` 配置项依据不同的 reader 插件而有所些微不同，但大部分的配置大同小异，特别是针对关系型数据库而言，其基本配置如下：

```json
{
  "name": "mysqlreader",
  "parameter": {
    "username": "",
    "password": "",
    "column": [],
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
其中 `name` 是插件的名称，每个插件的名称都是唯一的，每个插件更详细的配置可以参考[读取插件](reader)章节的各插件内容

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
    "connection": [
      {
        "jdbcUrl": "",
        "table": []
      }
    ]
  }
}
```

同样的，这里的 `name` 也是唯一的，每个插件更详细的配置可以参考[写入插件](writer)章节的各插件内容
