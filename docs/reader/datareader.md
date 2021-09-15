# Data Reader

`DataReader` 插件是专门提供用于开发和测试环境中，生产满足一定规则要求的数据的插件。

在实际开发和测试中，我们需要按照一定的业务规则来生产测试数据，而不仅仅是随机内容，比如身份证号码，银行账号，股票代码等。

## 为什么要重复发明轮子

诚然，网络上有相当多的专门的数据生产工具，其中不乏功能强大、性能也强悍。 但这些工具大部分是考虑到了数据生成这一段，而忽略了数据写入到目标端的问题，或者说有些考虑到了，但仅仅只考虑了一种或有限的几种数据库。

恰好 Addax 工具能够提供足够多的目标端写入能力，加上之前的已有的 [streamReader](streamreader) 已经算是一个简单版的数据生成工具，因此在此功能上 增加一些特定规则，再利用写入端多样性的能力，自然就成为了一个较好的数据生成工具。

## 配置示例

这里我把目前插件支持的规则全部列举到下面的例子中

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
          "name": "datareader",
          "parameter": {
            "column": [
              {
                "value": "1,100,",
                "rule": "random",
                "type": "double"
              },
              {
                "value": "DataX",
                "type": "string"
              },
              {
                "value": "1",
                "rule": "incr",
                "type": "long"
              },
              {
                "value": "1989/06/04 00:00:01,-1",
                "rule": "incr",
                "type": "date",
                "dateFormat": "yyyy/MM/dd hh:mm:ss"
              },
              {
                "value": "test",
                "type": "bytes"
              },
              {
                "rule": "address"
              },
              {
                "rule": "bank"
              },
              {
                "rule": "company"
              },
              {
                "rule": "creditCard"
              },
              {
                "rule": "debitCard"
              },
              {
                "rule": "idCard"
              },
              {
                "rule": "lat"
              },
              {
                "rule": "lng"
              },
              {
                "rule": "name"
              },
              {
                "rule": "job"
              },
              {
                "rule": "phone"
              },
              {
                "rule": "stockCode"
              },
              {
                "rule": "stockAccount"
              }
            ],
            "sliceRecordCount": 10
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

保存上述内容到 `job/datareader2stream.json`

然后执行该任务，其输出结果类似如下：

```shell
$ bin/addax.sh job/datareader2stream.json

  ___      _     _
 / _ \    | |   | |
/ /_\ \ __| | __| | __ ___  __
|  _  |/ _` |/ _` |/ _` \ \/ /
| | | | (_| | (_| | (_| |>  <
\_| |_/\__,_|\__,_|\__,_/_/\_\

:: Addax version ::    (v4.0.2-SNAPSHOT)

2021-08-13 17:02:00.888 [        main] INFO  VMInfo               - VMInfo# operatingSystem class => com.sun.management.internal.OperatingSystemImpl
2021-08-13 17:02:00.910 [        main] INFO  Engine               -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"column":[
						{
							"rule":"random",
							"type":"double",
							"scale": "2",
							"value":"1,100,"
						},
						{
							"type":"string",
							"value":"DataX"
						},
						{
							"rule":"incr",
							"type":"long",
							"value":"1"
						},
						{
							"dateFormat":"yyyy/MM/dd hh:mm:ss",
							"rule":"incr",
							"type":"date",
							"value":"1989/06/04 00:00:01,-1"
						},
						{
							"type":"bytes",
							"value":"test"
						},
						{
							"rule":"address"
						},
						{
							"rule":"bank"
						},
						{
							"rule":"company"
						},
						{
							"rule":"creditCard"
						},
						{
							"rule":"debitCard"
						},
						{
							"rule":"idCard"
						},
						{
							"rule":"lat"
						},
						{
							"rule":"lng"
						},
						{
							"rule":"name"
						},
						{
							"rule":"job"
						},
						{
							"rule":"phone"
						},
						{
							"rule":"stockCode"
						},
						{
							"rule":"stockAccount"
						}
					],
					"sliceRecordCount":10
				},
				"name":"datareader"
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

2021-08-13 17:02:00.937 [        main] INFO  PerfTrace            - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-08-13 17:02:00.938 [        main] INFO  JobContainer         - Addax jobContainer starts job.
2021-08-13 17:02:00.940 [        main] INFO  JobContainer         - Set jobId = 0
2021-08-13 17:02:00.976 [       job-0] INFO  JobContainer         - Addax Reader.Job [datareader] do prepare work .
2021-08-13 17:02:00.977 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do prepare work .
2021-08-13 17:02:00.978 [       job-0] INFO  JobContainer         - Job set Channel-Number to 1 channels.
2021-08-13 17:02:00.979 [       job-0] INFO  JobContainer         - Addax Reader.Job [datareader] splits to [1] tasks.
2021-08-13 17:02:00.980 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] splits to [1] tasks.
2021-08-13 17:02:01.002 [       job-0] INFO  JobContainer         - Scheduler starts [1] taskGroups.
2021-08-13 17:02:01.009 [ taskGroup-0] INFO  TaskGroupContainer   - taskGroupId=[0] start [1] channels for [1] tasks.
2021-08-13 17:02:01.017 [ taskGroup-0] INFO  Channel              - Channel set byte_speed_limit to -1, No bps activated.
2021-08-13 17:02:01.017 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.

7.65	DataX	1	1989-06-04 00:00:01	test	天津市南京县长寿区光明路263号	交通银行	易动力信息有限公司	6227894836568607	6235712610856305437	450304194808316766	31.3732613	-125.3507716	龚军	机电工程师	13438631667	726929	8741848665
18.58	DataX	2	1989-06-03 00:00:01	test	江苏省太原市浔阳区东山路33号	中国银行	时空盒数字信息有限公司	4096666711928233	6217419359154239015	220301200008188547	48.6648764	104.8567048	匡飞	化妆师	18093137306	006845	1815787371
16.16	DataX	3	1989-06-02 00:00:01	test	台湾省邯郸市清河区万顺路10号	大同商行	开发区世创科技有限公司	4096713966912225	6212977716107080594	150223196408276322	29.0134395	142.6426842	支波	审核员	13013458079	020695	3545552026
63.89	DataX	4	1989-06-01 00:00:01	test	上海市辛集县六枝特区甘园路119号	中国农业银行	泰麒麟传媒有限公司	6227893481508780	6215686558778997167	220822196208286838	-71.6484635	111.8181273	敬坤	房地产客服	13384928291	174445	0799668655
79.18	DataX	5	1989-05-31 00:00:01	test	陕西省南京市朝阳区大胜路170号	内蒙古银行	晖来计算机信息有限公司	6227535683896707	6217255315590053833	350600198508222018	-24.9783587	78.017024	蒋杨	固定资产会计	18766298716	402188	9633759917
14.97	DataX	6	1989-05-30 00:00:01	test	海南省长春县璧山区碧海街147号	华夏银行	浙大万朋科技有限公司	6224797475369912	6215680436662199846	220122199608190275	-3.5088667	-40.2634359	边杨	督导/巡店	13278765923	092780	2408887582
45.49	DataX	7	1989-05-29 00:00:01	test	台湾省潜江县梁平区七星街201号	晋城商行	开发区世创信息有限公司	5257468530819766	6213336008535546044	141082197908244004	-72.9200596	120.6018163	桑明	系统工程师	13853379719	175864	8303448618
8.45	DataX	8	1989-05-28 00:00:01	test	海南省杭州县城北区天兴路11号	大同商行	万迅电脑科技有限公司	6227639043120062	6270259717880740332	430405198908214042	-16.5115338	-39.336119	覃健	人事总监	13950216061	687461	0216734574
15.01	DataX	9	1989-05-27 00:00:01	test	云南省惠州市和平区海鸥街201号	内蒙古银行	黄石金承信息有限公司	6200358843233005	6235730928871528500	130300195008312067	-61.646097	163.0882369	卫建华	电话采编	15292600492	001658	1045093445
55.14	DataX	10	1989-05-26 00:00:01	test	辽宁省兰州市徐汇区东山街176号	廊坊银行	创汇科技有限公司	6227605280751588	6270262330691012025	341822200908168063	77.2165746	139.5431377	池浩	多媒体设计	18693948216	201678	0692522928

2021-08-13 17:02:04.020 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-08-13 17:02:04.021 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do post work.
2021-08-13 17:02:04.022 [       job-0] INFO  JobContainer         - Addax Reader.Job [datareader] do post work.
2021-08-13 17:02:04.025 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-08-13 17:02:04.028 [       job-0] INFO  StandAloneJobContainerCommunicator - Total 10 records, 1817 bytes | Speed 605B/s, 3 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2021-08-13 17:02:04.030 [       job-0] INFO  JobContainer         -
任务启动时刻                    : 2021-08-13 17:02:00
任务结束时刻                    : 2021-08-13 17:02:04
任务总计耗时                    :                  3s
任务平均流量                    :              605B/s
记录写入速度                    :              3rec/s
读出记录总数                    :                  10
读写失败总数                    :                   0
```

## 配置说明

`column` 的配置和其他插件的配置稍有不同，一个字段由以下配置项组成

| 配置项  |  是否必须   | 默认值    | 示例     | 说明             |
| -------| -----------| ---------|----------|-----------------|
| value  | 否         | 无       |  `Addax`   | 数据值，在某些情况下为必选项         |
| rule   | 否         |  `constant` | `idCard` | 数据生产规则，详细下面的描述    |
| type   |  否        |  `string`   | `double`  |  数据值类型    |
| dateFormat | 否     |   `yyyy-MM-dd HH:mm:ss` | `yyyy/MM/dd HH:mm:ss` | 日期格式，仅在`type` 为 `date` 时有效 |

### rule 说明

该插件的字段配置核心是 `rule` 字段，它用来指示应该生成什么样的数据，并依据不同规则，配合其他配置选项来生产满足期望的数据。 当前 `rule` 的配置均为内置支持的规则，暂不支持自定义，以下详细说明

### constant

`constant` 是 `rule` 的默认配置，该规则意味着要生成的数据值由 `value` 配置项决定，其不做任何变更。比如

```json
{
  "value": "Addax",
  "type": "string",
  "rule": "constant"
}
```

表示该字段生产的数据值均为 `Addax`

### incr

`incr` 配置项的含义和 `streamreader` 插件中的 `incr` 含义一致，表示这是一个递增的数据生产规则，比如

```json
{
  "value": "1,2",
  "rule": "incr",
  "type": "long"
}
```

表示该字段的数据是一个长整形，数值从 1 开始，每次递增 2，也就是形成 1 开始，步长为 2 的递增数列。

该字段更详细的配置规则和注意事项，可以参考 [streamreader](streamreader) 中的 `incr` 说明。

### random

`random` 配置项的含义和 `streamreader` 插件中的 `random` 含义一致，表示这是一个递增的数据生产规则，比如

```json
{
  "value": "1,10",
  "rule": "random",
  "type": "string"
}
```

表示该字段的数据是一个长度为 1 到 10 （1和10都包括）随机字符串。

该字段更详细的配置规则和注意事项，可以参考 [streamreader](streamreader) 中的 `random` 说明。

| 规则名称  | 含义   | 示例    | 数据类型 | 说明 |
|----------|-------|---------|--------|----------|
| `address` | 随机生成一条基本满足国内实际情况的地址信息 | `辽宁省兰州市徐汇区东山街176号` |string|  |
| `bank` | 随机生成一个国内银行名称 | `华夏银行` | string |  |
| `company` | 随机生成一个公司的名称 | `万迅电脑科技有限公司` | string |  |
| `creditCard` | 随机生成一个信用卡卡号 | `430405198908214042` | string | 16位 |
| `debitCard` | 随机生成一个储蓄卡卡号 | `6227894836568607` | string | 19位 |
| `idCard` | 随机生成一个国内身份证号码 | `350600198508222018` | string | 18位，负责校验规则，头6位编码满足行政区划要求 |
| `lat` | 随机生成维度数据 |`48.6648764`|double| 固定7位小数 ，也可以用`latitude` 表示 |
| `lng` |随机生成经度数据|`120.6018163`|double| 固定7位小数，也可以使用`longitude` 表示 |
| `name` |随机生成一个国内名字|`池浩`|string| 暂没考虑姓氏在国内的占比度 |
| `job` |随机生成一个国内岗位名称|`系统工程师`|string| 数据来源于招聘网站 |
| `phone` |随机生成一个国内手机号码|`15292600492`|string| 暂不考虑虚拟手机号 |
| `stockCode` |随机生成一个6位的股票代码|`687461`|string| 前两位满足国内股票代码编号规范 |
| `stockAccount` |随机生成一个10位的股票交易账户|`0692522928`|string| 完全随机，不满足账户规范 |

注意：上述表格中的规则返回的数据类型是固定的，且不支持修改，因此 `type` 无需配置，配置的类型也会被忽略，因为数据生成来自内部规则，所以 `value` 也无需配置，配置的内容也会被忽略。
