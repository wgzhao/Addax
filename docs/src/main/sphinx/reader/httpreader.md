# HttpReader

HttpReader 插件实现了读取Restful API 数据的能力

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
{
  "result": [
    {
      "CURR_DATE": "2019-12-09",
      "DEPT": "9700",
      "TOTAL_MANAGED_MARKET_VALUE": 1581.03,
      "TOTAL_MANAGED_MARKET_VALUE_GROWTH": 36.75,
      "TMMARKET_VALUE_DOD_GROWTH_RATE": -0.009448781026677719,
      "TMMARKET_VALUE_GROWTH_MON": -0.015153586011995693,
      "TMMARKET_VALUE_GROWTH_YEAR": 0.0652347643813081,
      "TMMARKET_VALUE_SHARECOM": 0.024853621341525287,
      "TMMARKET_VALUE_SHARE_GROWTH_RATE": -0.005242133578517903,
      "AVERAGE_NEW_ASSETS_DAYINMON": 1645.1193961136973,
      "YEAR_NEW_ASSET_SSHARECOM": 0.16690149257388515,
      "YN_ASSET_SSHARECOM_GROWTH_RATE": 0.017886267801303465,
      "POTENTIAL_LOST_ASSETS": 56.76,
      "TOTAL_LIABILITIES": 57.81,
      "TOTAL_ASSETS": 1306.33,
      "TOTAL_ASSETS_DOD_GROWTH": 4.79,
      "TOTAL_ASSETS_DOD_GROWTH_RATE": -0.006797058194980485,
      "NEW_ASSETS_DAY": 14.92,
      "NEW_ASSETS_MON": 90.29,
      "NEW_ASSETS_YEAR": 297.32,
      "NEW_ASSETS_DOD_GROWTH_RATE": -0.04015576541561927,
      "NEW_FUNDS_DAY": 18.16,
      "INFLOW_FUNDS_DAY": 2.12,
      "OUTFLOW_FUNDS_DAY": 9.73,
      "OVERALL_POSITION": 0.810298404938773,
      "OVERALL_POSITION_DOD_GROWTH_RATE": -0.03521615634095476,
      "NEW_CUST_FUNDS_MON": 69.44,
      "INFLOW_FUNDS_MONTH": 62.26,
      "OUTFLOW_FUNDS_MONTH": 32.59
    },
    {
      "CURR_DATE": "2019-08-30",
      "DEPT": "8700",
      "TOTAL_MANAGED_MARKET_VALUE": 1596.74,
      "TOTAL_MANAGED_MARKET_VALUE_GROWTH": 41.86,
      "TMMARKET_VALUE_DOD_GROWTH_RATE": 0.03470208565515685,
      "TMMARKET_VALUE_GROWTH_MON": 0.07818120801111743,
      "TMMARKET_VALUE_GROWTH_YEAR": -0.05440250244736409,
      "TMMARKET_VALUE_SHARECOM": 0.09997733019626448,
      "TMMARKET_VALUE_SHARE_GROWTH_RATE": -0.019726478499825697,
      "AVERAGE_NEW_ASSETS_DAYINMON": 1007.9314679742108,
      "YEAR_NEW_ASSET_SSHARECOM": 0.15123738798885086,
      "YN_ASSET_SSHARECOM_GROWTH_RATE": 0.04694052069678048,
      "POTENTIAL_LOST_ASSETS": 52.48,
      "TOTAL_LIABILITIES": 55.28,
      "TOTAL_ASSETS": 1366.72,
      "TOTAL_ASSETS_DOD_GROWTH": 10.12,
      "TOTAL_ASSETS_DOD_GROWTH_RATE": 0.009708491982487952,
      "NEW_ASSETS_DAY": 12.42,
      "NEW_ASSETS_MON": 41.14,
      "NEW_ASSETS_YEAR": 279.32,
      "NEW_ASSETS_DOD_GROWTH_RATE": -0.025878627161898062,
      "NEW_FUNDS_DAY": 3.65,
      "INFLOW_FUNDS_DAY": 14.15,
      "OUTFLOW_FUNDS_DAY": 17.08,
      "OVERALL_POSITION": 0.9098432997243932,
      "OVERALL_POSITION_DOD_GROWTH_RATE": 0.02111922282868306,
      "NEW_CUST_FUNDS_MON": 57.21,
      "INFLOW_FUNDS_MONTH": 61.16,
      "OUTFLOW_FUNDS_MONTH": 15.83
    },
    {
      "CURR_DATE": "2019-06-30",
      "DEPT": "6501",
      "TOTAL_MANAGED_MARKET_VALUE": 1506.72,
      "TOTAL_MANAGED_MARKET_VALUE_GROWTH": -13.23,
      "TMMARKET_VALUE_DOD_GROWTH_RATE": -0.0024973354204176554,
      "TMMARKET_VALUE_GROWTH_MON": -0.015530793150701896,
      "TMMARKET_VALUE_GROWTH_YEAR": -0.08556724628979398,
      "TMMARKET_VALUE_SHARECOM": 0.15000077963967678,
      "TMMARKET_VALUE_SHARE_GROWTH_RATE": -0.049629446804825755,
      "AVERAGE_NEW_ASSETS_DAYINMON": 1250.1040863177336,
      "YEAR_NEW_ASSET_SSHARECOM": 0.19098445630488178,
      "YN_ASSET_SSHARECOM_GROWTH_RATE": -0.007881179708853471,
      "POTENTIAL_LOST_ASSETS": 50.53,
      "TOTAL_LIABILITIES": 56.62,
      "TOTAL_ASSETS": 1499.53,
      "TOTAL_ASSETS_DOD_GROWTH": 29.56,
      "TOTAL_ASSETS_DOD_GROWTH_RATE": -0.02599813232345556,
      "NEW_ASSETS_DAY": 28.81,
      "NEW_ASSETS_MON": 123.24,
      "NEW_ASSETS_YEAR": 263.63,
      "NEW_ASSETS_DOD_GROWTH_RATE": 0.0073986669331394875,
      "NEW_FUNDS_DAY": 18.52,
      "INFLOW_FUNDS_DAY": 3.26,
      "OUTFLOW_FUNDS_DAY": 6.92,
      "OVERALL_POSITION": 0.8713692113306709,
      "OVERALL_POSITION_DOD_GROWTH_RATE": 0.02977644553289545,
      "NEW_CUST_FUNDS_MON": 85.14,
      "INFLOW_FUNDS_MONTH": 23.35,
      "OUTFLOW_FUNDS_MONTH": 92.95
    },
    {
      "CURR_DATE": "2019-12-07",
      "DEPT": "8705",
      "TOTAL_MANAGED_MARKET_VALUE": 1575.85,
      "TOTAL_MANAGED_MARKET_VALUE_GROWTH": 8.94,
      "TMMARKET_VALUE_DOD_GROWTH_RATE": -0.04384846980627058,
      "TMMARKET_VALUE_GROWTH_MON": -0.022962456288549656,
      "TMMARKET_VALUE_GROWTH_YEAR": -0.005047009316021089,
      "TMMARKET_VALUE_SHARECOM": 0.07819484815809447,
      "TMMARKET_VALUE_SHARE_GROWTH_RATE": -0.008534369960890256,
      "AVERAGE_NEW_ASSETS_DAYINMON": 1340.0339240689955,
      "YEAR_NEW_ASSET_SSHARECOM": 0.19019952857677042,
      "YN_ASSET_SSHARECOM_GROWTH_RATE": 0.01272353909992914,
      "POTENTIAL_LOST_ASSETS": 54.63,
      "TOTAL_LIABILITIES": 53.17,
      "TOTAL_ASSETS": 1315.08,
      "TOTAL_ASSETS_DOD_GROWTH": 49.31,
      "TOTAL_ASSETS_DOD_GROWTH_RATE": 0.0016538407028265922,
      "NEW_ASSETS_DAY": 29.17,
      "NEW_ASSETS_MON": 44.75,
      "NEW_ASSETS_YEAR": 172.87,
      "NEW_ASSETS_DOD_GROWTH_RATE": 0.045388692595736746,
      "NEW_FUNDS_DAY": 18.46,
      "INFLOW_FUNDS_DAY": 12.93,
      "OUTFLOW_FUNDS_DAY": 10.38,
      "OVERALL_POSITION": 0.8083127036694828,
      "OVERALL_POSITION_DOD_GROWTH_RATE": -0.02847453515632541,
      "NEW_CUST_FUNDS_MON": 49.74,
      "INFLOW_FUNDS_MONTH": 81.93,
      "OUTFLOW_FUNDS_MONTH": 18.17
    }
  ]
}
```

我们需要把 `result` 结果中的部分 key 值数据获取

### 配置

以下配置实现从接口获取数据并打印到终端

```
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
          "name": "httpreader",
          "parameter": {
            "connection": [
              {
                "url": "http://127.0.0.1:9090/mock/17/LDJSC/ASSET",
                "proxy": {
                    "host": "http://127.0.0.1:3128",
                    "auth": "user:pass"
                }
              }
            ],
            "reqParams": {
              "CURR_DATE":"2021-01-18",
              "DEPT":"9700"
            },
            "resultKey":"result",
            "method": "GET",
            "column": ["CURR_DATE","DEPT","TOTAL_MANAGED_MARKET_VALUE","TOTAL_MANAGED_MARKET_VALUE_GROWTH"],
            "username": "user",
            "password": "passw0rd",
            "headers": {
                "X-Powered-by": "Addax"
            }
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": "true"
          }
        }
      }
    ]
  }
}
```

将上述内容保存为 `job/httpreader2stream.json` 文件。

### 执行

执行以下命令，进行采集

```shell
bin/addax.py job/httpreader2stream.json
```

上述命令的输出结果大致如下：

```
2021-01-20 09:07:41.864 [main] INFO  VMInfo - VMInfo# operatingSystem class => com.sun.management.internal.OperatingSystemImpl
2021-01-20 09:07:41.877 [main] INFO  Engine - the machine info  =>

	osInfo: 	Mac OS X x86_64 10.15.1
	jvmInfo:	AdoptOpenJDK 14 14.0.2+12
	cpu num:	8

	totalPhysicalMemory:	-0.00G
	freePhysicalMemory:	-0.00G
	maxFileDescriptorCount:	-1
	currentOpenFileDescriptorCount:	-1

	GC Names	[G1 Young Generation, G1 Old Generation]

	MEMORY_NAME                    | allocation_size                | init_size
	CodeHeap 'profiled nmethods'   | 117.21MB                       | 2.44MB
	G1 Old Gen                     | 2,048.00MB                     | 39.00MB
	G1 Survivor Space              | -0.00MB                        | 0.00MB
	CodeHeap 'non-profiled nmethods' | 117.21MB                       | 2.44MB
	Compressed Class Space         | 1,024.00MB                     | 0.00MB
	Metaspace                      | -0.00MB                        | 0.00MB
	G1 Eden Space                  | -0.00MB                        | 25.00MB
	CodeHeap 'non-nmethods'        | 5.57MB                         | 2.44MB


2021-01-20 09:07:41.903 [main] INFO  Engine -
{
	"content":[
		{
			"reader":{
				"parameter":{
					"reqParams":{
						"CURR_DATE":"2021-01-18",
						"DEPT":"9700"
					},
					"method":"GET",
					"column":[
						"CURR_DATE",
						"DEPT",
						"TOTAL_MANAGED_MARKET_VALUE",
						"TOTAL_MANAGED_MARKET_VALUE_GROWTH"
					],
					"resultKey":"result",
					"connection":[
						{
							"url":"http://127.0.0.1:9090/mock/17/LDJSC/ASSET"
						}
					]
				},
				"name":"httpreader"
			},
			"writer":{
				"parameter":{
					"print":"true"
				},
				"name":"streamwriter"
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

2021-01-20 09:07:41.926 [main] INFO  PerfTrace - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-01-20 09:07:41.927 [main] INFO  JobContainer - Addax jobContainer starts job.
2021-01-20 09:07:41.928 [main] INFO  JobContainer - Set jobId = 0
2021-01-20 09:07:42.002 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] taskId[0] attemptCount[1] is started

2019-08-30	9700	1539.85	-14.78
2019-10-01	9700	1531.71	47.66
2020-12-03	9700	1574.38	7.34
2020-11-31	9700	1528.13	41.62
2019-03-01	9700	1554.28	-9.29

2021-01-20 09:07:45.006 [job-0] INFO  JobContainer -
任务启动时刻                    : 2021-01-20 09:07:41
任务结束时刻                    : 2021-01-20 09:07:44
任务总计耗时                    :                  3s
任务平均流量                    :               42B/s
记录写入速度                    :              1rec/s
读出记录总数                    :                   5
读写失败总数                    :                   0
```

### 参数说明

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

#### proxy

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

### 限制说明

1. 返回的结果必须是JSON类型
2. 当前所有key的值均当作字符串类型
3. 暂不支持接口Token鉴权模式
4. 暂不支持分页获取
5. 代理仅支持 `http` 模式
