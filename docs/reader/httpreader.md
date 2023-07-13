# Http Reader

HttpReader 插件实现了读取 Restful API 数据的能力

## 示例

### 示例接口与数据

以下配置演示了如何从一个指定的 API 中获取数据，假定访问的接口为：

<http://127.0.0.1:9090/mock/17/LDJSC/ASSET>

接口接受 GET 请求，请求的参数有

| 参数名称      | 参数值示例      |
|-----------|------------|
| CURR_DATE | 2021-01-17 |
| DEPT      | 9400       |
| USERNAME  | andi       |

以下是访问的数据样例，（实际返回数据略有不同）

```json
--8<-- "sql/http.json"
```

我们需要把 `result` 结果中的部分 key 值数据获取

### 配置

以下配置实现从接口获取数据并打印到终端

=== "job/httpreader2stream.json"

```json
--8<-- "jobs/httpreader.json"
```

将上述内容保存为 `job/httpreader2stream.json` 文件。

### 执行

执行以下命令，进行采集

```shell
bin/addax.sh job/httpreader2stream.json
```

上述命令的输出结果大致如下：

```
--8<-- "output/httpreader.txt"
```

## 参数说明

| 配置项        | 是否必须 |  数据类型   | 默认值 | 说明                                  |
|------------|:----:|:-------:|:---:|-------------------------------------|
| url        |  是   | string  |  无  | 要访问的 HTTP 地址                        |
| reqParams  |  否   |   map   |  无  | 接口请求参数                              |
| resultKey  |  否   | string  |  无  | 要获取结果的那个 key 值，如果是获取整个返回值，则可以不用填写   |
| method     |  否   | string  | get | 请求模式，仅支持 GET，POST 两种，不区分大小写         |
| column     |  是   |  list   |  无  | 要获取的 key，如果配置为 `"*"` ，则表示获取所有 key 值 |
| username   |  否   | string  |  无  | 接口请求需要的认证帐号(如有)                     |
| password   |  否   | string  |  无  | 接口请求需要的密码(如有)                       |
| proxy      |  否   |   map   |  无  | 代理地址,详见下面描述                         |
| headers    |  否   |   map   |  无  | 定制的请求头信息                            |
| isPage     |  否   | boolean |  无  | 接口是否分支分页（`4.1.1` 引入)                |
| pageParams |  否   |   map   |  无  | 分页参数(`4.1.1` 引入)                    |

### proxy

如果访问的接口需要通过代理，则可以配置 `proxy` 配置项，该配置项是一个 json 字典，包含一个必选的 `host`
字段和一个可选的 `auth` 字段。

```json
{
  "proxy": {
    "host": "http://127.0.0.1:8080",
    "auth": "user:pass"
  }
}
```

如果是 `sock` 代理 (V4,v5)，则可以写

```json
{
  "proxy": {
    "host": "socks://127.0.0.1:8080",
    "auth": "user:pass"
  }
}
```

`host` 是代理地址，包含代理类型，目前仅支持 `http` 代理和 `socks`(V4, V5 均可) 代理。 如果代理需要认证，则可以配置 `auth` ,
它由用户名和密码组成，两者之间用冒号(`:`) 隔开。

### column

`column` 除了直接指定 key 之外，还允许用 JSON Xpath 风格来指定需要获取的 key 值，假定你要读取的 JSON 文件如下：

```json
{
  "result": [
    {
      "CURR_DATE": "2019-12-09",
      "DEPT": {
        "ID": "9700"
      },
      "KK": [
        {
          "COL1": 1
        },
        {
          "COL2": 2
        }
      ]
    },
    {
      "CURR_DATE": "2021-11-09",
      "DEPT": {
        "ID": "6500"
      },
      "KK": [
        {
          "COL1": 3
        },
        {
          "COL2": 4
        }
      ]
    }
  ]
}
```

我们希望把 `CURR_DATE`, `ID`, `COL1`, `COL2` 当作四个字段读取，那么你的 `column` 可以这样配置：

```json
{
  "column": [
    "CURR_DATE",
    "DEPT.ID",
    "KK[0].COL1",
    "KK[1].COL2"
  ]
}
```

其执行结果如下：

```shell
...
2021-10-30 14:01:50.273 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.

2019-12-09	9700	1	2
2021-11-09	6500	3	4

2021-10-30 14:01:53.283 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-10-30 14:01:53.284 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do post work.
2021-10-30 14:01:53.284 [       job-0] INFO  JobContainer         - Addax Reader.Job [httpreader] do post work.
2021-10-30 14:01:53.286 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-10-30 14:01:53.289 [       job-0] INFO  JobContainer         -
任务启动时刻                    : 2021-10-30 14:01:50
任务结束时刻                    : 2021-10-30 14:01:53
任务总计耗时                    :                  3s
任务平均流量                    :               10B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   2
读写失败总数                    :                   0
```

注意： 如果你指定了不存在的 Key，则直接返回为 NULL 值。

### isPage

`isPage` 参数用来指定接口是否分页，它是一个布尔值，如果为 `true` 则表示接口分页，否则表示不分页。

当接口支持分页时，该直接会自动分页读取，直到接口返回的最后一次返回的数据的记录数小于每页的记录数为止。

### pageParams

`pageParams` 参数仅在 `isPage` 参数为 `true` 时生效，它是一个 JSON 字典，包含两个可选字段 `pageIndex` 和 `pageSize` 。

`pageIndex` 用来表示用于分页指示的当前页面，他是一个 JSON 字段，包含两个可选字段 `key` 和 `value` ，其中 `key` 用来指定表示页码的参数名，`value` 用来指定当前页码的值。

`pageSize` 用来表示用于分页指示的每页大小，他是一个 JSON 字段，包含两个可选字段 `key` 和 `value` ，其中 `key` 用来指定表示每页大小的参数名，`value` 用来指定每页大小的值。

这两个参数的默认值如下：

```json
{
  "pageParams": {
    "pageIndex": {
      "key": "pageIndex",
      "value": 1
    },
    "pageSize": {
      "key": "pageSize",
      "value": 100
    }
  }
}
```

如果你的接口分页参数不是 `pageIndex` 和 `pageSize` ，则可以通过 `pageParams` 参数来指定。比如

```json
{
  "isPage": true,
  "pageParams": {
    "pageIndex": {
      "key": "page",
      "value": 1
    },
    "pageSize": {
      "key": "size",
      "value": 100
    }
  }
}
```

这表示你传递给接口的分页参数为 `page=1&size=100` 。

## 限制说明

1. 返回的结果必须是 JSON 类型
2. 当前所有 key 的值均当作字符串类型
3. 暂不支持接口 Token 鉴权模式
4. 暂不支持分页获取
5. 代理仅支持 `http` 模式
