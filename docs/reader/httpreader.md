# Http Reader

HttpReader 插件实现了读取 Restful API 数据的能力

## 示例

### 示例接口与数据

以下配置演示了如何从一个指定的 API 中获取数据，假定访问的接口为：

<http://127.0.0.1:9090/mock/17/LDJSC/ASSET>

走 GET 请求，请求的参数有

| 参数名称 | 参数值示例 |
|---------|----------|
| CURR_DATE | 2021-01-17 |
| DEPT | 9400 |
| USERNAME | andi |

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

| 配置项    | 是否必须 | 数据类型 | 默认值 | 说明                                                        |
| --------- | :------: | :------: | :----: | ----------------------------------------------------------- |
| url       |    是    |  string  |   无   | 要访问的HTTP地址                                            |
| reqParams |    否    |   map    |   无   | 接口请求参数                                                |
| resultKey |    否    |  string  |   无   | 要获取结果的那个key值，如果是获取整个返回值，则可以不用填写 |
| method    |    否    |  string  |  get   | 请求模式，仅支持GET，POST两种，不区分大小写                 |
| column    |    是    |   list   |   无   | 要获取的key，如果配置为 `"*"` ，则表示获取所有key的值       |
| username  |    否    |   string |  无    | 接口请求需要的认证帐号（如有) |
| password  |    否    |   string |  无    | 接口请求需要的密码（如有) |
| proxy     |    否    |  map     | 无     | 代理地址,详见下面描述    |
| headers   |    否    |  map     | 无     | 定制的请求头信息 |

### proxy

如果访问的接口需要通过代理，则可以配置 `proxy` 配置项，该配置项是一个 json 字典，包含一个必选的 `host` 字段和一个可选的 `auth` 字段。

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

`host` 是代理地址，包含代理类型，目前仅支持 `http` 代理和 `socks`(V4, V5均可) 代理。 如果代理需要认证，则可以配置  `auth` , 它由 用户名和密码组成，两者之间用冒号(:) 隔开。

## 限制说明

1. 返回的结果必须是JSON类型
2. 当前所有key的值均当作字符串类型
3. 暂不支持接口Token鉴权模式
4. 暂不支持分页获取
5. 代理仅支持 `http` 模式
