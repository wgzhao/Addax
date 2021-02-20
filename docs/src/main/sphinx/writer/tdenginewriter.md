# TDengineWriter

TDengineWriter 插件实现了将数据写入到涛思公司的 [TDengine](https://www.taosdata.com/cn/) 数据库系统。在底层实现上，TDengineWriter 通过JDBC JNI 驱动连接远程 TDengine 数据库，
并执行相应的sql语句将数据批量写入 TDengine 库中。

## 前置条件

考虑到性能问题，该插件使用了 TDengine 的 JDBC-JNI 驱动， 该驱动直接调用客户端 API（libtaos.so 或 taos.dll）将写入和查询请求发送到taosd 实例。因此在使用之前需要配置好动态库链接文件。

首先将 `plugin/writer/tdenginewriter/libs/libtaos.so.2.0.16.0` 拷贝到 `/usr/lib64` 目录，然后执行下面的命令创建软链接

```shell
ln -sf /usr/lib64/libtaos.so.2.0.16.0 /usr/lib64/libtaos.so.1
ln -sf /usr/lib64/libtaos.so.1 /usr/lib64/libtaos.so
```

## 示例

假定要写入的表如下：

```sql
create table test.datax_test (
    ts timestamp,
    name nchar(100),
    file_size int,
    file_date timestamp,
    flag_open bool,
    memo nchar(100)
);
```

以下是配置文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column" : [
              {
                "random":"2017-08-01 00:01:02,2020-01-01 12:13:14",
                "type": "date"
              },
              {
                "value": "DataX",
                "type": "string"
              },
              {
                "value": 19880808,
                "type": "long"
              },
              {
                "value": "1988-08-08 08:08:08",
                "type": "date"
              },
              {
                "value": true,
                "type": "bool"
              },
              {
                "value": "test",
                "type": "bytes"
              }
            ],
            "sliceRecordCount": 1000
          }
        },
        "writer": {
          "name": "tdenginewriter",
          "parameter": {
            "username": "root",
            "password": "taosdata",
            "column": ["ts", "name", "file_size", "file_date", "flag_open", "memo" ],
            "connection": [
              {
                "jdbcUrl": "jdbc:TAOS://127.0.0.1:6030/test",
                "table": [ "datax_test"]
              }
            ]
          }
        }
      }
    ]
  }
}
```

将上述配置文件保存为   `job/stream2tdengine.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/datax.py job/tdengine2stream.json
```

命令输出类似如下：

```
2021-02-20 15:52:07.691 [main] INFO  VMInfo - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2021-02-20 15:52:07.748 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"column":[
						{
							"random":"2017-08-01 00:01:02,2020-01-01 12:13:14",
							"type":"date"
						},
						{
							"type":"string",
							"value":"DataX"
						},
						{
							"type":"long",
							"value":19880808
						},
						{
							"type":"date",
							"value":"1988-08-08 08:08:08"
						},
						{
							"type":"bool",
							"value":true
						},
						{
							"type":"bytes",
							"value":"test"
						}
					],
					"sliceRecordCount":1000
				},
				"name":"streamreader"
			},
			"writer":{
				"parameter":{
					"password":"*****",
					"column":[
						"ts",
						"name",
						"file_size",
						"file_date",
						"flag_open",
						"memo"
					],
					"connection":[
						{
							"jdbcUrl":"jdbc:TAOS://127.0.0.1:6030/test",
							"table":[
								"datax_test"
							]
						}
					],
					"username":"root",
					"preSql":[]
				},
				"name":"tdenginewriter"
			}
		}
	],
	"setting":{
		"speed":{
			"bytes":-1,
			"channel":1
		}
	}
}

2021-02-20 15:52:07.786 [main] INFO  PerfTrace - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-02-20 15:52:07.787 [main] INFO  JobContainer - DataX jobContainer starts job.
2021-02-20 15:52:07.789 [main] INFO  JobContainer - Set jobId = 0
java.library.path:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
2021-02-20 15:52:08.048 [job-0] INFO  OriginalConfPretreatmentUtil - table:[datax_test] all columns:[ts,name,file_size,file_date,flag_open,memo].
2021-02-20 15:52:08.056 [job-0] INFO  OriginalConfPretreatmentUtil - Write data [
INSERT INTO %s (ts,name,file_size,file_date,flag_open,memo) VALUES(?,?,?,?,?,?)
], which jdbcUrl like:[jdbc:TAOS://127.0.0.1:6030/test]

2021-02-20 15:52:11.158 [job-0] INFO  JobContainer -
任务启动时刻                    : 2021-02-20 15:52:07
任务结束时刻                    : 2021-02-20 15:52:11
任务总计耗时                    :                  3s
任务平均流量                    :           11.07KB/s
记录写入速度                    :            333rec/s
读出记录总数                    :                1000
读写失败总数                    :                   0
```

## 参数说明

| 配置项          | 是否必须 | 类型  | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |-------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息，注意，这里的 `TAOS` 必须大写 ｜
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述见[rdbmswriter](rdbmswriter.md) ｜
| preSql         |    否    | list  | 无     | 数据写入钱先执行的sql语句，例如清除旧数据,如果 Sql 中有你需要操作到的表名称，可用 `@table` 表示 |
| postSql        |   否      | list | 无    | 数据写入完成后执行的sql语句，例如加上某一个时间戳|
| batchSize       |    否    | int | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 DataX 出现OOM或者目标数据库事务提交失败导致挂起 |


## 类型转换

目前 TDenginereader 支持 TDengine 所有类型，具体如下

| DataX 内部类型| TDengine 数据类型    |
| -------- | -----  |
| Long     | SMALLINT, TINYINT, INT, BIGINT, TIMESTAMP |
| Double   | FLOAT, DOUBLE|
| String   |  BINARY, NCHAR |
| Boolean  | BOOL   |

## 当前支持版本

TDengine 2.0.16

## 注意事项

- TDengine JDBC-JNI 驱动和动态库版本要求一一匹配，因此如果你的数据版本并不是 `2.0.16`，则需要同时替换动态库和插件目录中的JDBC驱动
- TDengine 的时序字段（timestamp）默认最小值为 `1500000000000`，即 `2017-07-14 10:40:00.0`，如果你写入的时许时间戳小于该值，则会报错

