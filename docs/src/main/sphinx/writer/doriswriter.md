# DorisWriter

DorisWriter 插件实现了以流式方式加载数据到 [Doris](http://doris.incubator.apache.org/master/zh-CN/) ，其实现上是通过访问 Doris http 连接(8030)，然后通过 [stream load](http://doris.incubator.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)
加载数据到数据中，相比 `insert into` 方式效率要高不少，也是官方推荐的生产环境下的数据加载方式。

Doris 是一个兼容 MySQL 协议的数据库后端，因此读取 Doris 可以使用 [MySQLReader](../reader/mysqlreader.html) 进行访问。 

## 示例

假定要写入的表的建表语句如下：

```sql
CREATE DATABASE example_db;
CREATE TABLE example_db.table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

下面配置一个从内存读取数据，然后写入到 doris 表的配置文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      }
    },
    "content": [
      {
        "writer": {
          "name": "doriswriter",
          "parameter": {
            "username": "test",
            "password": "123456",
            "batchSize": 1024,
            "connection": [
              {
                "table": "table1",
                "database": "example_db",
                "endpoint": "http://127.0.0.1:8030/"
              }
            ]
          }
        },
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "random": "1,500",
                "type": "long"
              },
              {
                "random": "1,127",
                "type": "long"
              },
              {
                "value": "this is a text",
                "type": "string"
              },
              {
                "random": "5,200",
                "type": "long"
              }
            ],
            "sliceRecordCount": 100
          }
        }
      }
    ]
  }
}
```

将上述配置文件保存为 `job/stream2doris.json`

执行下面的命令

```shell
bin/addax.py job/stream2doris.json
```

输出类似如下：

```
2021-02-23 15:22:57.851 [main] INFO  VMInfo - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2021-02-23 15:22:57.871 [main] INFO  Engine -
{
"content":[
{
"reader":{
    "parameter":{
            "column":[
                    {
                            "random":"1,500",
                            "type":"long"
                    },
                    {
                            "random":"1,127",
                            "type":"long"
                    },
                    {
                            "type":"string",
                            "value":"username"
                    }
            ],
            "sliceRecordCount":100
    },
    "name":"streamreader"
},
"writer":{
    "parameter":{
            "password":"*****",
            "batchSize":1024,
            "connection":[
                    {
                            "database":"example_db",
                            "endpoint":"http://127.0.0.1:8030/",
                            "table":"table1"
                    }
            ],
            "username":"test"
    },
    "name":"doriswriter"
}
}
],
"setting":{
"speed":{
"channel":2
}
}
}

2021-02-23 15:22:57.886 [main] INFO  PerfTrace - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-02-23 15:22:57.886 [main] INFO  JobContainer - Addax jobContainer starts job.
2021-02-23 15:22:57.920 [job-0] INFO  JobContainer - Scheduler starts [1] taskGroups.
2021-02-23 15:22:57.928 [taskGroup-0] INFO  TaskGroupContainer - taskGroupId=[0] start [2] channels for [2] tasks.
2021-02-23 15:22:57.935 [taskGroup-0] INFO  Channel - Channel set byte_speed_limit to -1, No bps activated.
2021-02-23 15:22:57.936 [taskGroup-0] INFO  Channel - Channel set record_speed_limit to -1, No tps activated.
2021-02-23 15:22:57.970 [0-0-1-writer] INFO  DorisWriterTask - connect DorisDB with http://127.0.0.1:8030//api/example_db/table1/_stream_load
2021-02-23 15:22:57.970 [0-0-0-writer] INFO  DorisWriterTask - connect DorisDB with http://127.0.0.1:8030//api/example_db/table1/_stream_load

2021-02-23 15:23:00.941 [job-0] INFO  JobContainer - PerfTrace not enable!
2021-02-23 15:23:00.946 [job-0] INFO  JobContainer -
任务启动时刻                    : 2021-02-23 15:22:57
任务结束时刻                    : 2021-02-23 15:23:00
任务总计耗时                    :                  3s
任务平均流量                    :            1.56KB/s
记录写入速度                    :             66rec/s
读出记录总数                    :                 200
读写失败总数                    :                   0
```

## 参数说明

| 配置项          | 是否必须 | 类型  | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |-------|
| endpoint         |    是    | string | 无     | Doris 的HTTP连接方式，只需要写到主机和端口即可，具体路径插件会自动拼装 ｜
| username        |    是    | string | 无     | HTTP 签名验证帐号 |
| password        |    否    | string | 无     | HTTP 签名验证密码 |
| table           |    是    | string | 无     | 所选取的需要同步的表名|
| column          |    否    | list | 无     |  所配置的表中需要同步的列名集合，详细描述见[rdbmswriter](rdbmswriter.md) ｜
| batchSize       |    否    | int | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起 |

### column

该插件中的 `column` 不是必须项，如果没有配置该项，或者配置为 `["*"]` ， 则按照 reader 插件获取的字段值进行顺序拼装。
否则可以按照如下方式指定需要插入的字段

```json
{
  "column": ["siteid","citycode","username"]
}
```

