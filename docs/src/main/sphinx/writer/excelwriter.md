# Excel Writer

`excelwriter` 实现了将数据写入到 Excel 文件的功能

## 配置示例

我们假定从内存读取数据，并写入到 Excel 文件中

```json
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
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "DataX",
                "type": "string"
              },
              {
                "value": 19890604,
                "type": "long"
              },
              {
                "value": "1989-06-04 11:22:33",
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
          "name": "excelwriter",
          "parameter": {
            "path": "/tmp/out",
            "fileName": "test",
            "header":["str", "长度", "日期", "是否为真", "字节类型"]
          }
        }
      }
    ]
  }
}
```

讲上述内容保存为 `job/stream2excel.json`

执行下面的命令：

```shell
bin/addax.sh job/stream2excel.sh
```

应该得到类似如下的输出

<details>
<summary>点击展开</summary>

```shell

  ___      _     _
 / _ \    | |   | |
/ /_\ \ __| | __| | __ ___  __
|  _  |/ _` |/ _` |/ _` \ \/ /
| | | | (_| | (_| | (_| |>  <
\_| |_/\__,_|\__,_|\__,_/_/\_\

:: Addax version ::    (v4.0.3-SNAPSHOT)

2021-09-10 22:16:38.247 [        main] INFO  VMInfo               - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2021-09-10 22:16:38.269 [        main] INFO  Engine               -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"column":[
						{
							"type":"string",
							"value":"DataX"
						},
						{
							"type":"long",
							"value":19890604
						},
						{
							"type":"date",
							"value":"1989-06-04 11:22:33"
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
					"path":"/tmp/out",
					"fileName":"test",
					"header":[
						"str",
						"长度",
						"日期",
						"是否为真",
						"字节类型"
					],
					"writeMode":"truncate"
				},
				"name":"excelwriter"
			}
		}
	],
	"setting":{
		"speed":{
			"byte":-1,
			"channel":1
		}
	}
}

2021-09-10 22:16:38.287 [        main] INFO  PerfTrace            - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-09-10 22:16:38.287 [        main] INFO  JobContainer         - Addax jobContainer starts job.
2021-09-10 22:16:38.289 [        main] INFO  JobContainer         - Set jobId = 0
2021-09-10 22:16:38.303 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] do prepare work .
2021-09-10 22:16:38.304 [       job-0] INFO  JobContainer         - Addax Writer.Job [excelwriter] do prepare work .
2021-09-10 22:16:38.304 [       job-0] INFO  JobContainer         - Job set Channel-Number to 1 channels.
2021-09-10 22:16:38.304 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] splits to [1] tasks.
2021-09-10 22:16:38.305 [       job-0] INFO  JobContainer         - Addax Writer.Job [excelwriter] splits to [1] tasks.
2021-09-10 22:16:38.325 [       job-0] INFO  JobContainer         - Scheduler starts [1] taskGroups.
2021-09-10 22:16:38.332 [ taskGroup-0] INFO  TaskGroupContainer   - taskGroupId=[0] start [1] channels for [1] tasks.
2021-09-10 22:16:38.335 [ taskGroup-0] INFO  Channel              - Channel set byte_speed_limit to -1, No bps activated.
2021-09-10 22:16:38.336 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.
2021-09-10 22:16:41.345 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-09-10 22:16:41.346 [       job-0] INFO  JobContainer         - Addax Writer.Job [excelwriter] do post work.
2021-09-10 22:16:41.346 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] do post work.
2021-09-10 22:16:41.348 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-09-10 22:16:41.349 [       job-0] INFO  StandAloneJobContainerCommunicator - Total 1000 records, 26000 bytes | Speed 8.46KB/s, 333 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.528s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2021-09-10 22:16:41.350 [       job-0] INFO  JobContainer         -
任务启动时刻                    : 2021-09-10 22:16:38
任务结束时刻                    : 2021-09-10 22:16:41
任务总计耗时                    :                  3s
任务平均流量                    :            8.46KB/s
记录写入速度                    :            333rec/s
读出记录总数                    :                1000
读写失败总数                    :                   0
```
</details>

## 参数说明

| 配置项    | 是否必须 | 类型          | 默认值 | 描述                             |
| :------- | -------- | ----------- | ------ | -------------------------------- |
| path     | 是      | string      | 无   | 指定文件保存的目录, 指定的目录如果不存在，则尝试创建   |
| fileName | 是      | string      | 无   | 要生成的excel 文件名，详述如下  |
| header   | 否      | list        | 无   | Excel 表头   |

### fileName

如果配置的 `fileName` 没有后缀，则自动加上 `.xlsx`；
如果后缀为 `.xls`，则报错，因为当前仅生成 Excel 97 以后的文件格式，即 `.xlsx` 后缀的文件

### header

如果不指定 `header` ，则生成的 Excel 文件没有表头，只有数据。
注意，插件不关心 header 的数量是否匹配数据中的列数，也就是说表头的列数并不要求和接下来的数据的列数相等。

## 限制

1. 当前仅生成一个 Excel 文件，且没有考虑行数和列数是否超过了 Excel 的限定
2. 如果指定的目录下有同名文件，当前会被覆盖，后续会统一处理目标目录的问题
3. 当前日期格式的数据，设置单元格样式为 `yyyy-MM-dd HH:mm:ss`，且不能定制
4. 不支持二进制类型的数据写入