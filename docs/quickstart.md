# 快速使用

## 安装 Addax

如果你不想编译，你可以执行下面的命令，直接从下载已经编译好的二进制文件

```shell
curl -sS -o addax-4.0.2.tar.gz https://github.com/wgzhao/Addax/releases/download/4.0.2/addax-4.0.2.tar.gz`

tar -xzf addax-4.0.2.tar.gz
cd addax-4.0.2
```

或者你可以自行下载源代码进行编译 

```shell
git clone https://github.com/wgzhao/addax.git
cd addax
git checkout 4.0.2
mvn clean package -pl '!:addax-docs'
mvn package assembly:single
cd target/addax/addax-4.0.2
```

## 开始第一个采集任务


要使用 `Addax` 进行数据采集，只需要编写一个任务采集文件，该文件为 JSON 格式，以下是一个简单的配置文件，该任务的目的是从内存读取读取指定内容的数据，并将其打印出来。

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": 1
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "addax",
                "type": "string"
              },
              {
                "value": 19890604,
                "type": "long"
              },
              {
                "value": "1989-06-04 00:00:00",
                "type": "date"
              },
              {
                "value": true,
                "type": "bool"
              }
            ],
            "sliceRecordCount": 10
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

将上述文件保存为 `job/test.json`

然后执行下面的命令：

```shell
bin/addax.sh job/test.json
```

如果没有报错，应该会有类似这样的输出

```
  ___      _     _
 / _ \    | |   | |
/ /_\ \ __| | __| | __ ___  __
|  _  |/ _` |/ _` |/ _` \ \/ /
| | | | (_| | (_| | (_| |>  <
\_| |_/\__,_|\__,_|\__,_/_/\_\

:: Addax version ::    (v4.0.3-SNAPSHOT)

2021-08-23 13:45:17.199 [        main] INFO  VMInfo               - VMInfo# operatingSystem class => com.sun.management.internal.OperatingSystemImpl
2021-08-23 13:45:17.223 [        main] INFO  Engine               -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"column":[
						{
							"type":"string",
							"value":"addax"
						},
						{
							"type":"long",
							"value":19890604
						},
						{
							"type":"date",
							"value":"1989-06-04 00:00:00"
						},
						{
							"type":"bool",
							"value":true
						}
					],
					"sliceRecordCount":10
				},
				"name":"streamreader"
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
			"byte":-1,
			"channel":1
		}
	}
}

2021-08-23 13:45:17.238 [        main] INFO  PerfTrace            - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-08-23 13:45:17.239 [        main] INFO  JobContainer         - Addax jobContainer starts job.
2021-08-23 13:45:17.240 [        main] INFO  JobContainer         - Set jobId = 0
2021-08-23 13:45:17.250 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] do prepare work .
2021-08-23 13:45:17.250 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do prepare work .
2021-08-23 13:45:17.251 [       job-0] INFO  JobContainer         - Job set Channel-Number to 1 channels.
2021-08-23 13:45:17.251 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] splits to [1] tasks.
2021-08-23 13:45:17.252 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] splits to [1] tasks.
2021-08-23 13:45:17.276 [       job-0] INFO  JobContainer         - Scheduler starts [1] taskGroups.
2021-08-23 13:45:17.282 [ taskGroup-0] INFO  TaskGroupContainer   - taskGroupId=[0] start [1] channels for [1] tasks.
2021-08-23 13:45:17.287 [ taskGroup-0] INFO  Channel              - Channel set byte_speed_limit to -1, No bps activated.
2021-08-23 13:45:17.288 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
addax	19890604	1989-06-04 00:00:00	true
2021-08-23 13:45:20.295 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-08-23 13:45:20.296 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do post work.
2021-08-23 13:45:20.297 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] do post work.
2021-08-23 13:45:20.302 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-08-23 13:45:20.305 [       job-0] INFO  StandAloneJobContainerCommunicator - Total 10 records, 220 bytes | Speed 73B/s, 3 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.011s | Percentage 100.00%
2021-08-23 13:45:20.307 [       job-0] INFO  JobContainer         -
任务启动时刻                    : 2021-08-23 13:45:17
任务结束时刻                    : 2021-08-23 13:45:20
任务总计耗时                    :                  3s
任务平均流量                    :               73B/s
记录写入速度                    :              3rec/s
读出记录总数                    :                  10
读写失败总数                    :                   0
```

接下来，你可以继续了解如何[配置一个采集任务文件](setupJob)
