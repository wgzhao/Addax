# Stream Writer

StreamWriter 是一个将数据写入内存的插件，一般用来将获取到的数据写到终端，用来调试读取插件的数据处理情况。

一个典型的 streamwriter 配置如下：

```json
{
  "name": "streamwriter",
  "parameter": {
    "encoding": "UTF-8",
    "print": true
  }
}
```

上述配置会将获取的数据直接打印到终端。 该插件也支持将数据写入到文件，配置如下：

```json
{
  "name": "streamwriter",
  "parameter": {
    "encoding": "UTF-8",
    "path": "/tmp/out",
    "fileName": "out.txt",
    "fieldDelimiter": ",",
    "recordNumBeforeSleep": "100",
    "sleepTime": "5"
  }
}
```

上述配置中:

- `fieldDelimiter` 表示字段分隔符，默认为制表符(`\t`)
- `recordNumBeforeSleep` 表示获取多少条记录后，执行休眠，默认为0，表示不启用该功能
- `sleepTime` 则表示休眠多长时间，单位为秒，默认为0，表示不启用该功能。

上述配置的含义是将数据写入到 `/tmp/out/out.txt` 文件， 每获取100条记录后，休眠5秒。
