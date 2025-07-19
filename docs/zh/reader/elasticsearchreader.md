# ElasticSearchReader

ElasticSearchReader 插件实现了从 [Elasticsearch](https://www.elastic.co/cn/elasticsearch/) 读取索引的功能， 它通过 Elasticsearch 提供的 Rest API （默认端口9200），执行指定的查询语句批量获取数据

## 示例

假定要获取的索引内容如下

```json
--8<-- "sql/es.json"
```

配置一个从 Elasticsearch 读取数据并打印到终端的任务

=== "job/es2stream.json"

  ```json
  --8<-- "jobs/esreader.json"
  ```

将上述内容保存为 `job/es2stream.json`

执行下面的命令进行采集

```shell
bin/addax.sh job/es2stream.json
```

其输出结果类似如下（输出记录数有删减)

```
--8<-- "output/esreader.txt"
```

## 参数说明

| 配置项      | 是否必须 | 类型    | 默认值                 | 描述                                               |
| :---------- | :------: | ------- | ---------------------- | -------------------------------------------------- |
| endpoint    |    是    | string  | 无                     | ElasticSearch的连接地址                            |
| accessId    |    否    | string  | `""`                   | http auth中的user                                  |
| accessKey   |    否    | string  | `""`                   | http auth中的password                              |
| index       |    是    | string  | 无                     | elasticsearch中的index名                           |
| type        |    否    | string  | index名                | elasticsearch中index的type名                       |
| search      |    是    | list    | `[]`                   | json格式api搜索数据体                              |
| column      |    是    | list    | 无                     | 需要读取的字段                                     |
| timeout     |    否    | int     | 60                     | 客户端超时时间(单位：秒)                          |
| discovery   |    否    | boolean | false                  | 启用节点发现(轮询)并定期更新客户机中的服务器列表 |
| compression |    否    | boolean | true                   | http请求，开启压缩                                 |
| multiThread |    否    | boolean | true                   | http请求，是否有多线程                             |
| searchType  |    否    | string  | `dfs_query_then_fetch` | 搜索类型                                           |
| headers     |    否    | map     | `{}`                   | http请求头                                         |
| scroll      |    否    | string  | `""`                   | 滚动分页配置                                       |

### search

search 配置项允许配置为满足 Elasticsearch API 查询要求的内容，比如这样：

```json
{
  "query": {
    "match": {
      "message": "myProduct"
    }
  },
  "aggregations": {
    "top_10_states": {
      "terms": {
        "field": "state",
        "size": 10
      }
    }
  }
}
```

### searchType

searchType 目前支持以下几种：

- dfs_query_then_fetch
- query_then_fetch
- count
- scan
