# ElasticSearchReader

ElasticSearchReader 插件实现了从 [Elasticsearch](https://www.elastic.co/cn/elasticsearch/) 读取索引的功能， 它通过 Elasticsearch 提供的 Rest API （默认端口9200），执行指定的查询语句批量获取数据

## 示例

假定要获取的索引内容如下

```
{
"took": 14,
"timed_out": false,
"_shards": {
"total": 1,
"successful": 1,
"skipped": 0,
"failed": 0
},
"hits": {
"total": 2,
"max_score": 1,
"hits": [
{
"_index": "test-1",
"_type": "default",
"_id": "38",
"_score": 1,
"_source": {
"col_date": "2017-05-25T11:22:33.000+08:00",
"col_integer": 19890604,
"col_keyword": "hello world",
"col_ip": "1.1.1.1",
"col_text": "long text",
"col_double": 19890604,
"col_long": 19890604,
"col_geo_point": "41.12,-71.34"
}
},
{
"_index": "test-1",
"_type": "default",
"_id": "103",
"_score": 1,
"_source": {
"col_date": "2017-05-25T11:22:33.000+08:00",
"col_integer": 19890604,
"col_keyword": "hello world",
"col_ip": "1.1.1.1",
"col_text": "long text",
"col_double": 19890604,
"col_long": 19890604,
"col_geo_point": "41.12,-71.34"
}
}
]
}
}
```

配置一个从 Elasticsearch 读取数据并打印到终端的任务

```
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "elasticsearchreader",
          "parameter": {
            "endpoint": "http://127.0.0.1:9200",
            "accessId":"",
            "accesskey":"",
            "index": "test-1",
            "type": "default",
            "searchType": "dfs_query_then_fetch",
            "headers": {},
            "scroll": "3m",
            "search": [
              {
                "query": {
                  "match": {
                    "col_ip": "1.1.1.1"
                  }
                },
                "aggregations": {
                  "top_10_states": {
                    "terms": {
                      "field": "col_date",
                      "size": 10
                    }
                  }
                }
              }
            ],
            "column": ["col_ip", "col_double", "col_long","col_integer",
              "col_keyword", "col_text","col_geo_point","col_date"
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true,
            "encoding": "UTF-8"
          }
        }
      }
    ]
  }
}
```

将上述内容保存为 `job/es2stream.json`

执行下面的命令进行采集

```shell
bin/addax.sh job/es2stream.json
```

其输出结果类似如下（输出记录数有删减)

```
2021-02-19 13:38:15.860 [main] INFO  VMInfo - VMInfo# operatingSystem class => com.sun.management.internal.OperatingSystemImpl
2021-02-19 13:38:15.895 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"accessId":"",
					"headers":{},
					"endpoint":"http://127.0.0.1:9200",
					"search":[
                      {
                        "query": {
                          "match": {
                            "col_ip": "1.1.1.1"
                          }
                        },
                        "aggregations": {
                          "top_10_states": {
                            "terms": {
                              "field": "col_date",
                              "size": 10
                            }
                          }
                        }
                      }
					],
					"accesskey":"*****",
					"searchType":"dfs_query_then_fetch",
					"scroll":"3m",
					"column":[
						"col_ip",
						"col_double",
						"col_long",
						"col_integer",
						"col_keyword",
						"col_text",
						"col_geo_point",
						"col_date"
					],
					"index":"test-1",
					"type":"default"
				},
				"name":"elasticsearchreader"
			},
			"writer":{
				"parameter":{
					"print":true,
					"encoding":"UTF-8"
				},
				"name":"streamwriter"
			}
		}
	],
	"setting":{
		"errorLimit":{
			"record":0,
			"percentage":0.02
		},
		"speed":{
			"byte":-1,
			"channel":1
		}
	}
}

2021-02-19 13:38:15.934 [main] INFO  PerfTrace - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-02-19 13:38:15.934 [main] INFO  JobContainer - Addax jobContainer starts job.
2021-02-19 13:38:15.937 [main] INFO  JobContainer - Set jobId = 0

2017-05-25T11:22:33.000+08:00	19890604	hello world	1.1.1.1	long text	19890604	19890604	41.12,-71.34
2017-05-25T11:22:33.000+08:00	19890604	hello world	1.1.1.1	long text	19890604	19890604	41.12,-71.34

2021-02-19 13:38:19.845 [job-0] INFO  AbstractScheduler - Scheduler accomplished all tasks.
2021-02-19 13:38:19.848 [job-0] INFO  JobContainer - Addax Writer.Job [streamwriter] do post work.
2021-02-19 13:38:19.849 [job-0] INFO  JobContainer - Addax Reader.Job [elasticsearchreader] do post work.
2021-02-19 13:38:19.855 [job-0] INFO  JobContainer - PerfTrace not enable!
2021-02-19 13:38:19.858 [job-0] INFO  StandAloneJobContainerCommunicator - Total 95 records, 8740 bytes | Speed 2.84KB/s, 31 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.103s | Percentage 100.00%
2021-02-19 13:38:19.861 [job-0] INFO  JobContainer -
任务启动时刻                    : 2021-02-19 13:38:15
任务结束时刻                    : 2021-02-19 13:38:19
任务总计耗时                    :                  3s
任务平均流量                    :            2.84KB/s
记录写入速度                    :             31rec/s
读出记录总数                    :                   2
读写失败总数                    :                   0
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
| timeout     |    否    | int     | 60                     | 客户端超时时间（单位：秒)                          |
| discovery   |    否    | boolean | false                  | 启用节点发现将(轮询)并定期更新客户机中的服务器列表 |
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
