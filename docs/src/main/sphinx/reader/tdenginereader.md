# TDengineReader 

TDengineReader 插件实现了从涛思公司的 [TDengine](https://www.taosdata.com/cn/) 读取数据。在底层实现上，TDengineReader 通过JDBC JNI 驱动连接远程 TDengine 数据库，
并执行相应的sql语句将数据从 TDengine 库中批量获。

不同于其他关系型数据库，TDengine 不支持FetchSize

## 前置条件

考虑到性能问题，该插件使用了 TDengine 的 JDBC-JNI 驱动， 该驱动直接调用客户端 API（libtaos.so 或 taos.dll）将写入和查询请求发送到taosd 实例。因此在使用之前需要配置好动态库链接文件。

首先将 `plugin/reader/tdenginereader/libs/libtaos.so.2.0.16.0` 拷贝到 `/usr/lib64` 目录，然后执行下面的命令创建软链接

```shell
ln -sf /usr/lib64/libtaos.so.2.0.16.0 /usr/lib64/libtaos.so.1
ln -sf /usr/lib64/libtaos.so.1 /usr/lib64/libtaos.so
```

## 示例

TDengine 数据自带了一个演示数据库 [taosdemo](https://www.taosdata.com/cn/getting-started/)，我们从演示数据库读取部分数据并打印到终端

以下是配置文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "tdenginereader",
          "parameter": {
            "username": "root",
            "password": "taosdata",
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:TAOS://127.0.0.1:6030/test"
                ],
                "querySql": [
                  "select * from test.meters where ts <'2017-07-14 10:40:02' and  loc='beijing' limit 10"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": true
          }
        }
      }
    ]
  }
}
```

将上述配置文件保存为   `job/tdengine2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.py job/tdengine2stream.json
```

命令输出类似如下：

```
2021-02-20 15:32:23.161 [main] INFO  VMInfo - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2021-02-20 15:32:23.229 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"password":"*****",
					"connection":[
						{
							"querySql":[
								"select * from test.meters where ts <'2017-07-14 10:40:02' and  loc='beijing' limit 100"
							],
							"jdbcUrl":[
								"jdbc:TAOS://127.0.0.1:6030/test"
							]
						}
					],
					"username":"root"
				},
				"name":"tdenginereader"
			},
			"writer":{
				"parameter":{
					"print":true
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
			"channel":3
		}
	}
}

2021-02-20 15:32:23.277 [main] INFO  PerfTrace - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-02-20 15:32:23.278 [main] INFO  JobContainer - Addax jobContainer starts job.
2021-02-20 15:32:23.281 [main] INFO  JobContainer - Set jobId = 0
java.library.path:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
....
2021-02-20 15:32:23.687 [0-0-0-reader] INFO  CommonRdbmsReader$Task - Begin to read record by Sql: [select * from test.meters where ts <'2017-07-14 10:40:02' and  loc='beijing' limit 100
] jdbcUrl:[jdbc:TAOS://127.0.0.1:6030/test].
2021-02-20 15:32:23.692 [0-0-0-reader] WARN  DBUtil - current database does not supoort TYPE_FORWARD_ONLY/CONCUR_READ_ONLY
2021-02-20 15:32:23.740 [0-0-0-reader] INFO  CommonRdbmsReader$Task - Finished read record by Sql: [select * from test.meters where ts <'2017-07-14 10:40:02' and  loc='beijing' limit 100
] jdbcUrl:[jdbc:TAOS://127.0.0.1:6030/test].

1500000001000	5	5	0	1	beijing
1500000001000	0	6	2	1	beijing
1500000001000	7	0	0	1	beijing
1500000001000	8	9	6	1	beijing
1500000001000	9	9	1	1	beijing
1500000001000	8	2	0	1	beijing
1500000001000	4	5	5	3	beijing
1500000001000	3	3	3	3	beijing
1500000001000	5	4	8	3	beijing
1500000001000	9	4	6	3	beijing

2021-02-20 15:32:26.689 [job-0] INFO  JobContainer -
任务启动时刻                    : 2021-02-20 15:32:23
任务结束时刻                    : 2021-02-20 15:32:26
任务总计耗时                    :                  3s
任务平均流量                    :              800B/s
记录写入速度                    :             33rec/s
读出记录总数                    :                 100
读写失败总数                    :                   0
```

## 参数说明

| 配置项          | 是否必须 | 类型       | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |--------------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息，注意这里的 `TAOS` 必须大写 |
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述[rdbmreader](rdbmsreader.md) ｜
| where           |    否    | string | 无     | 针对表的筛选条件 |
| querySql        |    否    | list | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |


## 类型转换

目前 TDenginereader 支持 TDengine 所有类型，具体如下

| Addax 内部类型| TDengine 数据类型    |
| -------- | -----  |
| Long     | SMALLINT, TINYINT, INT, BIGINT, TIMESTAMP |
| Double   | FLOAT, DOUBLE|
| String   |  BINARY, NCHAR |
| Boolean  | BOOL   |

## 当前支持版本

TDengine 2.0.16

## 注意事项

- TDengine JDBC-JNI 驱动和动态库版本要求一一匹配，因此如果你的数据版本并不是 `2.0.16`，则需要同时替换动态库和插件目录中的JDBC驱动

