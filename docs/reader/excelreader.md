# Excel Reader

`excelreader` 插件实现了从 Microsoft Excel 文件读取数据的能力。

## 配置

### 获取样例文件

从[这里下载](https://www.dropbox.com/s/gocc927re76uirl/excel_reader_demo.zip?dl=1)用于演示的Excel压缩文件，并解压缩放置到 `/tmp/in` 目录下。
三个文件夹的内容相同，其中

- `demo.xlsx` 是 Excel 新格式
- `demo.xls` 是 Excel 老格式
- `demo_gbk.xlsx` 是在Windows下创建，已GBK编码存储的文件

文件内容，如下图所示：

![excel demo](../images/excel_demo.png)

表头大致说明了单元数据的特征

### 创建 job 文件

创建如下 json 文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "excelreader",
          "parameter": {
            "path": ["/tmp/in"],
            "header": true,
            "skipRows": 0
          }
        },
        "writer": {
          "parameter": {
            "print": true
          },
          "name": "streamwriter"
        }
      }
    ]
  }
}
```

将输出内容存保为到 `job/excel2stream.json` 文件中，执行采集命令：

```shell
$ bin/addax.sh job/excel2stream.json
```

如果没有报错，应该得到如下输出：

<details>
<summary>点击展开</summary>

```shell
  ___      _     _
 / _ \    | |   | |
/ /_\ \ __| | __| | __ ___  __
|  _  |/ _` |/ _` |/ _` \ \/ /
| | | | (_| | (_| | (_| |>  <
\_| |_/\__,_|\__,_|\__,_/_/\_\

:: Addax version ::    (v4.0.3)

2021-09-09 14:43:42.579 [        main] INFO  VMInfo               - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2021-09-09 14:43:42.621 [        main] INFO  Engine               -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"path":[
						"/tmp/in"
					],
					"column":[
						{
							"name":"no",
							"type":"long"
						},
						{
							"name":"birth",
							"format":"yyyy-MM-dd HH:mm:ss",
							"type":"date"
						},
						{
							"name":"kk",
							"type":"string"
						}
					],
					"header":true,
					"skipHeader":true,
					"encoding":"UTF-8",
					"fieldDelimiter":","
				},
				"name":"excelreader"
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
		"speed":{
			"bytes":-1,
			"channel":2
		}
	}
}

2021-09-09 14:43:42.653 [        main] INFO  PerfTrace            - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-09-09 14:43:42.653 [        main] INFO  JobContainer         - Addax jobContainer starts job.
2021-09-09 14:43:42.655 [        main] INFO  JobContainer         - Set jobId = 0
2021-09-09 14:43:42.669 [       job-0] INFO  ExcelReader$Job      - add file [/tmp/in/demo_old.xls] as a candidate to be read.
2021-09-09 14:43:42.669 [       job-0] INFO  ExcelReader$Job      - add file [/tmp/in/demo_gbk.xlsx] as a candidate to be read.
2021-09-09 14:43:42.670 [       job-0] INFO  ExcelReader$Job      - add file [/tmp/in/demo.xlsx] as a candidate to be read.
2021-09-09 14:43:42.670 [       job-0] INFO  ExcelReader$Job      - The number of files to read is: [3]
2021-09-09 14:43:42.677 [       job-0] INFO  JobContainer         - Addax Reader.Job [excelreader] do prepare work .
2021-09-09 14:43:42.678 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do prepare work .
2021-09-09 14:43:42.679 [       job-0] INFO  JobContainer         - Job set Channel-Number to 2 channels.
2021-09-09 14:43:42.681 [       job-0] INFO  JobContainer         - Addax Reader.Job [excelreader] splits to [3] tasks.
2021-09-09 14:43:42.682 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] splits to [3] tasks.
2021-09-09 14:43:42.727 [       job-0] INFO  JobContainer         - Scheduler starts [1] taskGroups.
2021-09-09 14:43:42.736 [ taskGroup-0] INFO  TaskGroupContainer   - taskGroupId=[0] start [2] channels for [3] tasks.
2021-09-09 14:43:42.741 [ taskGroup-0] INFO  Channel              - Channel set byte_speed_limit to -1, No bps activated.
2021-09-09 14:43:42.742 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.
2021-09-09 14:43:42.755 [0-0-1-reader] INFO  ExcelReader$Task     - The first row is skipped as a table header
2021-09-09 14:43:42.755 [0-0-1-reader] INFO  ExcelReader$Task     - begin read file /tmp/in/demo.xlsx
2021-09-09 14:43:42.757 [0-0-0-reader] INFO  ExcelReader$Task     - The first row is skipped as a table header
2021-09-09 14:43:42.758 [0-0-0-reader] INFO  ExcelReader$Task     - begin read file /tmp/in/demo_gbk.xlsx
1	11	1102.234	Addax加上中文	2021-09-10 00:00:00	5544.17	1102.234
1	12	1103.234	Addax加上中文	2021-09-11 00:00:00	5552.17	1103.234
1	13	1104.234	Addax加上中文	2021-09-12 00:00:00	5560.17	1104.234
1	14	1105.234	Addax加上中文	2021-09-13 00:00:00	5568.17	1105.234
1	15	1106.234	Addax加上中文	2021-09-14 00:00:00	5576.17	1106.234
1	16	1107.234	Addax加上中文	2021-09-15 00:00:00	5584.17	1107.234
1	17	1108.234	Addax加上中文	2021-09-16 00:00:00	5592.17	1108.234
1	18	1109.234	Addax加上中文	2021-09-17 00:00:00	5600.17	1109.234
1	19	1110.234	Addax加上中文	2021-09-18 00:00:00	5608.17	1110.234
1	20	1111.234	Addax加上中文	2021-09-19 00:00:00	5616.17	1111.234
1	21	1112.234	Addax加上中文	2021-09-20 00:00:00	5624.17	1112.234
1	22	1113.234	Addax加上中文	2021-09-21 00:00:00	5632.17	1113.234
1	23	1114.234	Addax加上中文	2021-09-22 00:00:00	5640.17	1114.234
1	24	1115.234	Addax加上中文	2021-09-23 00:00:00	5648.17	1115.234
1	25	1116.234	Addax加上中文	2021-09-24 00:00:00	5656.17	1116.234
1	26	1117.234	Addax加上中文	2021-09-25 00:00:00	5664.17	1117.234
1	27	1118.234	Addax加上中文	2021-09-26 00:00:00	5672.17	1118.234
1	28	1119.234	Addax加上中文	2021-09-27 00:00:00	5680.17	1119.234
1	29	1120.234	Addax加上中文	2021-09-28 00:00:00	5688.17	1120.234
1	30	1121.234	Addax加上中文	2021-09-29 00:00:00	5696.17	1121.234
1	11	1102.234	Addax加上中文	2021-09-10 00:00:00	5544.17	1102.234
2	12	1103.234	Addax加上中文	2021-09-11 00:00:00	5552.17	1103.234
3	13	1104.234	Addax加上中文	2021-09-12 00:00:00	5560.17	1104.234
4	14	1105.234	Addax加上中文	2021-09-13 00:00:00	5568.17	1105.234
5	15	1106.234	Addax加上中文	2021-09-14 00:00:00	5576.17	1106.234
6	16	1107.234	Addax加上中文	2021-09-15 00:00:00	5584.17	1107.234
7	17	1108.234	Addax加上中文	2021-09-16 00:00:00	5592.17	1108.234
8	18	1109.234	Addax加上中文	2021-09-17 00:00:00	5600.17	1109.234
9	19	1110.234	Addax加上中文	2021-09-18 00:00:00	5608.17	1110.234
10	20	1111.234	Addax加上中文	2021-09-19 00:00:00	5616.17	1111.234
11	21	1112.234	Addax加上中文	2021-09-20 00:00:00	5624.17	1112.234
12	22	1113.234	Addax加上中文	2021-09-21 00:00:00	5632.17	1113.234
13	23	1114.234	Addax加上中文	2021-09-22 00:00:00	5640.17	1114.234
14	24	1115.234	Addax加上中文	2021-09-23 00:00:00	5648.17	1115.234
15	25	1116.234	Addax加上中文	2021-09-24 00:00:00	5656.17	1116.234
16	26	1117.234	Addax加上中文	2021-09-25 00:00:00	5664.17	1117.234
17	27	1118.234	Addax加上中文	2021-09-26 00:00:00	5672.17	1118.234
18	28	1119.234	Addax加上中文	2021-09-27 00:00:00	5680.17	1119.234
19	29	1120.234	Addax加上中文	2021-09-28 00:00:00	5688.17	1120.234
20	30	1121.234	Addax加上中文	2021-09-29 00:00:00	5696.17	1121.234
2021-09-09 14:43:43.894 [0-0-2-reader] INFO  ExcelReader$Task     - The first row is skipped as a table header
2021-09-09 14:43:43.894 [0-0-2-reader] INFO  ExcelReader$Task     - begin read file /tmp/in/demo_old.xls
1	11	1102.234	Addax加上中文	2021-09-10 00:00:00	5544.17	1102.234
2	12	1103.234	Addax加上中文	2021-09-11 00:00:00	5552.17	1103.234
3	13	1104.234	Addax加上中文	2021-09-12 00:00:00	5560.17	1104.234
4	14	1105.234	Addax加上中文	2021-09-13 00:00:00	5568.17	1105.234
5	15	1106.234	Addax加上中文	2021-09-14 00:00:00	5576.17	1106.234
6	16	1107.234	Addax加上中文	2021-09-15 00:00:00	5584.17	1107.234
7	17	1108.234	Addax加上中文	2021-09-16 00:00:00	5592.17	1108.234
8	18	1109.234	Addax加上中文	2021-09-17 00:00:00	5600.17	1109.234
9	19	1110.234	Addax加上中文	2021-09-18 00:00:00	5608.17	1110.234
10	20	1111.234	Addax加上中文	2021-09-19 00:00:00	5616.17	1111.234
11	21	1112.234	Addax加上中文	2021-09-20 00:00:00	5624.17	1112.234
12	22	1113.234	Addax加上中文	2021-09-21 00:00:00	5632.17	1113.234
13	23	1114.234	Addax加上中文	2021-09-22 00:00:00	5640.17	1114.234
14	24	1115.234	Addax加上中文	2021-09-23 00:00:00	5648.17	1115.234
15	25	1116.234	Addax加上中文	2021-09-24 00:00:00	5656.17	1116.234
16	26	1117.234	Addax加上中文	2021-09-25 00:00:00	5664.17	1117.234
17	27	1118.234	Addax加上中文	2021-09-26 00:00:00	5672.17	1118.234
18	28	1119.234	Addax加上中文	2021-09-27 00:00:00	5680.17	1119.234
19	29	1120.234	Addax加上中文	2021-09-28 00:00:00	5688.17	1120.234
20	30	1121.234	Addax加上中文	2021-09-29 00:00:00	5696.17	1121.234
2021-09-09 14:43:45.753 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-09-09 14:43:45.754 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do post work.
2021-09-09 14:43:45.756 [       job-0] INFO  JobContainer         - Addax Reader.Job [excelreader] do post work.
2021-09-09 14:43:45.761 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-09-09 14:43:45.762 [       job-0] INFO  StandAloneJobContainerCommunicator - Total 60 records, 3360 bytes | Speed 1.09KB/s, 20 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.993s | Percentage 100.00%
2021-09-09 14:43:45.764 [       job-0] INFO  JobContainer         -
任务启动时刻                    : 2021-09-09 14:43:42
任务结束时刻                    : 2021-09-09 14:43:45
任务总计耗时                    :                  3s
任务平均流量                    :            1.09KB/s
记录写入速度                    :             20rec/s
读出记录总数                    :                  60
读写失败总数                    :                   0
```

</details>

## 参数说明

| 配置项    | 是否必须 | 类型          | 默认值 | 描述                             |
| :------- | -------- | ----------- | ------ | -------------------------------- |
| path     | 是       | string/list | 无     | 指定要读取的文件夹，可以指定多个 |
| header   | 否       | boolean     | false  | 文件是否包含头                   |
| skipRows | 否       | int         | 0      | 要跳过前多少行                   |

### header

Excel 文件是否包含头，如果包含，则跳过

### skipRows

指定要跳过的行数， 默认为0，表示不跳过。这里要注意的是，假定 设置了 `header` 为 true，同时设置 `skipRows` 为 2。则表示前三行都跳过。
如果 `header` 为 false， 则表示跳过前两行。

### 支持的数据类型

Excel 读取功能的实现依赖于 [Apache POI](https://poi.apache.org/) 项目，该实现对单元格的数据类型定义很宽泛。
仅定义了布尔型(Boolean)，数值型（Double)，字符串型(String) 三种。其中数值型包括所有整数，小数和日期。
目前对于数值类型做了简单区分

1. 使用库工具类探测是否为日期类型，如果是，则判断为日期类型
2. 将数值转换为长整形，并和原值比较，如果大小相等，则判断为长整型(Long)
3. 否则判断为浮点型（Double）

## 限制

1. 当前仅读取文件的第一个 Sheet 而忽略其他 Sheets
2. 暂不支持指定列读取
3. 暂不支持跳过尾部行数（比如有总结的尾行可能并不符合要求）
4. 暂不判断每一行的列数是否相等，需要 Excel 自行保证
5. 仅会读取指定目录下文件后缀为 `xlsx` 或 `xls` 的文件，其他后缀文件将会忽略并给出告警消息
