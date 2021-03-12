<p align="center">
    <img alt="DataX Logo" src="https://github.com/wgzhao/DataX/blob/master/docs/images/datax-logo.png?raw=true" width="205" />
</p>
<p align="center">DataX 是一个支持主流数据库的通用数据采集工具</p>
<p align="center"><a href="https://datax.readthedocs.io">使用文档</a> 详细描述了如何安装部署和每个采集插件的使用方法 </p>
<p align="center">
   <a href="https://github.com/wgzhao/DataX/workflows/Maven%20Package/badge.svg">
       <img src="https://github.com/wgzhao/DataX/workflows/Maven%20Package/badge.svg" alt="Maven Package" />
   </a>
   <a href="https://datax.readthedocs.io/zh_CN/latest/?badge=latest">
       <img src="https://readthedocs.org/projects/datax/badge/?version=latest" alt="Documentation Status" />
   </a>
</p>


该项目从阿里的[DataX](https://github.com/alibaba/datax) 而来，经过了精简和改造，说明如下

## 当前稳定版

`3.2.2`

注： 从 `3.2.1` 版本开始，包类名已经更改，因此不再兼容 `3.1.x` 版本

## 功能差别说明

### 精简

删除了仅限于阿里内部的数据库，这些数据库在非阿里集团无法使用，因此直接删除，包括：

- ADS
- DRDS
- OCS
- ODPS
- OSS
- OTS

### 增加

增加了部分插件，目前包括

#### reader plugin

1. clickhousereader
2. dbffilereader
3. hbase20xreader
4. jsonfilereader
5. kudureader
6. influxdbreader
7. httpreader
8. elastichsearchreader
9. tdenginereader

#### writer plugin

1. dbffilewrite
2. greenplumwriter
3. kuduwriter
4. influxdbwriter
5. tdenginewriter

### 部分插件增强功能，罗列如下

- 关系型数据库 增加了几乎所有基本数据类型和一部分复杂类型的支持
- hdfswriter 增加了对 Decimal 数据类型格式的支持
- hdfswriter 增加了对 Parquet 文件格式的支持
- hdfswrite 增加了目录覆盖模式
- hdfswriter 增加了更多的文件压缩格式支持
- hdfswriter 的临时目录位置改动为当前写入目录下的隐藏目录，解决了之前和写入目录平行导致的自动增加分区的问题
- hdfswriter 在覆盖模式下，改进了文件删除机制，减少了对应表查询为空的时间窗口
- hdfsreader 增加了对 Parquet 文件格式的支持
- hdfsreader 增加了更多的文件压缩格式支持
- hbasex11sqlwrite 增加了 Kerberos 支持
- oraclewriter 增加对 `merge into` 语法支持(感谢 @weihebu 提供的建议和参考)
- postgresqlwriter 增加 `insert into ... on conflict` 语法支持 (感谢 @weihebu 提供的建议和参考)
- rdbmsreader/rdbmswriter 增加了TDH Inceptor， Trino 查询引擎支持
- 尽可能减少了本地jar包的依赖，转为从maven仓库获取
- 绝大部分依赖包升级到了最新稳定版本，减少了潜在漏洞
- 不同插件下的相同依赖包做了版本统一

## 支持的数据库一览表

| 数据库或文件系统 | 读取   | 写入   | 插件名称(reader/writer)             | 备注                                                                                 |
| ---------------- | ------ | ------ | ----------------------------------- | ------------------------------------------------------------------------------------ |
| Cassander        | 支持   | 支持   | cassandrareader/cassandrawriter     |                                                                                      |
| ClickHouse       | 支持   | 支持   | clickhousereader/clickhousewriter   |                                                                                      |
| DB2              | 支持   | 支持   | rbdmsreader/rdbmswriter             | 理论上支持，但未实际测试                                                             |
| DBF              | 支持   | 支持   | dbffilereader/dbffilewriter         |                                                                                      |
| ElasticSearch    | 支持 | 支持     | elasticsearchreader/elasticsearchwriter | 原始代码来自[@Kestrong](https://github.com/Kestrong/datax-elasticsearch)               |
| FTP              | 支持   | 支持   | ftpreader/ftpwriter                 |                                                                                      |
| HBase 1.x        | 支持   | 支持   | hbase11xreader/hbase11xwriter       | 直接操作HBase                                                                        |
| HBase 1.x        | 支持   | 支持   | hbase11xsqlreader/hbase11xsqlwriter | 通过[Phoenix](https://phoenix.apache.org)操作HBase                                   |
| HBase 2.x        | 支持   | 不支持 | hbase20xreader                      | 直接操作HBase                                                                        |
| HBase 2.x        | 支持   | 支持   | hbase20xsqlreader/hbase20xsqlwriter | 通过[Phoenix](https://phoenix.apache.org)操作HBase                                   |
| HDFS             | 支持   | 支持   | hdfsreader/hdfswriter               | HDFS 2.x 以上版本                                                                    |
| HTTP             | 支持   | 不支持 | httpreader                          | 仅支持返回值为JSON类型的接口                                                         |
| Greenplum        | 支持   | 支持   | postgresqlreader/greenplumwriter    |                                                                                      |
| InfluxDB         | 支持   | 支持   | influxdbreader/influxdbwriter       | 仅支持1.x版本，2.0及以上暂不支持                                                     |
| json             | 支持   | 不支持 | jsonfilereader                      |                                                                                      |
| kudu             | 支持   | 支持   | kudureader/kuduwriter               | 通过原生接口，计划更新Impala连接                                                     |
| MongoDB          | 支持   | 支持   | mongodbreader/mongodbwriter         |                                                                                      |
| MySQL/MariaDB    | 支持   | 支持   | mysqlreader/mysqlwriter             |                                                                                      |
| Oracle           | 支持   | 支持   | oraclereader/oraclewriter           |                                                                                      |
| PostgreSQL       | 支持   | 支持   | postgresqlreader/postgresqlwriter   |                                                                                      |
| PrestoSQL        | 支持   | 支持   | rdbmsreader/rdbmswriter             | [trino(原PrestoSQL)](https://trino.io) 310以上                                       |
| Redis            | 支持   | 支持   | redisreader/rediswriter             |                                                                                      |
| SQL Server       | 支持   | 支持   | sqlserverreader/sqlserverwriter     |                                                                                    |
| TDengine         | 支持   | 支持   | tdenginereader/tdenginewriter       | 支持 [TDengine](https://www.taosdata.com/cn/) 数据库读写                              |
| TDH Inceptor2    | 支持   | 支持   | rdbmsreader/rdbmswriter             | [星环 TDH](http://transwarp.cn/transwarp/product-TDH.html?categoryId=18) 5.1以上版本 |
| TEXT             | 支持   | 支持   | textfilereader/textfilewriter       |                                                                                      |

## 快速开始

### 不想编译

如果你懒得编译或者因为环境无法编译，可以从以下链接下载对应的版本

| 版本   | 连接地址                                                     | md5值                             |
| ----- | ------------------------------------------------------------| ---------------------------------|
| 3.2.2 | https://pan.baidu.com/s/1TQyaERnIk9EQRDULfQE69w 提取码: jh31 | b04d2563adb36457b85e48c318757ea3 |
| 3.2.1 | https://pan.baidu.com/s/1as6sL09HlxAN8b2pZ1DttQ 提取码: hwgx | ecda4a961b032c75718502caf54246a8 |
| 3.1.9 | https://pan.baidu.com/s/1GYpehEvB-W3qnqilhskXFw 提取码: q4wv | 48c4104294cd9bb0c749efc50b32b4dd |
| 3.1.8 | https://pan.baidu.com/s/1jv-tb-11grYaUnsgnEhDzw 提取码: 2dnf | ef110ae1ea31e1761dc25d6930300485 |
| 3.1.7 | https://pan.baidu.com/s/1CE5I8V5TNptdOp6GLid3Jg 提取码: v5u3 | fecca6c4a32f2bf7246fdef8bc2912fe |
| 3.1.6 | https://pan.baidu.com/s/1Ldg10E3qWkbUT44rkH19og 提取码: 4av4 | f6aea7e0ce4b9ec83554e9c6d6ab3cb6 |
| 3.1.5 | https://pan.baidu.com/s/1yY_lJqulE6hKqktoQbbGmQ 提取码: 2r4p | 9ae27c1c434a097f67a17bb704f70731 |
| 3.1.4 | https://pan.baidu.com/s/1_plsvzD_GrWN-HffPBtz-g 提取码: kpjn | 7aca526fe7f6f0f54dc467f6ca1647b1 |
| 3.1.2 | https://pan.baidu.com/s/1zFqv8E6iJX549zdSZDQgiQ 提取码: 7jdk | 3674711fc9b68fad3086f3c8526a3427 |
| 3.1.1 | https://pan.baidu.com/s/1GwmFA7-hPkd6GKiZEvUKXg 提取码: 1inn | 0fa4e7902420704b2e814fef098f40ae |

### 编译及打包

```shell
git clone https://github.com/wgzhao/datax.git DataX
cd DataX
mvn clean package
mvn package assembly:single
```

如果需要编译文档，请执行下面的命令

```shell
cd docs
mvn clean package
```

编译打包成功后，会在项目目录的`target/datax` 目录下创建一个 `datax-<version>`的 文件夹，其中 `<version` 表示版本。

### 开始第一个任务

`job` 子目录包含了大量的任务样本，其中 `job.json` 可以作为冒烟测试，执行如下

```shell
cd target/datax/datax-<version>
python bin/datax.py job/job.json
```

上述命令的输出大致如下：
<details>
<summary>点击展开</summary>

```
 bin/datax.py job/job.json

DataX (DATAX-V3), From Alibaba !
Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved.


2020-09-23 19:51:30.990 [main] INFO  VMInfo - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2020-09-23 19:51:30.997 [main] INFO  Engine - the machine info  =>

	osInfo:	Oracle Corporation 1.8 25.181-b13
	jvmInfo:	Mac OS X x86_64 10.15.6
	cpu num:	4

	totalPhysicalMemory:	-0.00G
	freePhysicalMemory:	-0.00G
	maxFileDescriptorCount:	-1
	currentOpenFileDescriptorCount:	-1

	GC Names	[PS MarkSweep, PS Scavenge]

	MEMORY_NAME                    | allocation_size                | init_size
	PS Eden Space                  | 677.50MB                       | 16.00MB
	Code Cache                     | 240.00MB                       | 2.44MB
	Compressed Class Space         | 1,024.00MB                     | 0.00MB
	PS Survivor Space              | 2.50MB                         | 2.50MB
	PS Old Gen                     | 1,365.50MB                     | 43.00MB
	Metaspace                      | -0.00MB                        | 0.00MB


2020-09-23 19:51:31.009 [main] INFO  Engine -
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
							"value":"1989-06-04 00:00:00"
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
					"sliceRecordCount":10
				},
				"name":"streamreader"
			},
			"writer":{
				"parameter":{
					"print":true,
					"column":[
						"col1"
					],
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

2020-09-23 19:51:31.068 [main] WARN  Engine - prioriy set to 0, because NumberFormatException, the value is: null
2020-09-23 19:51:31.069 [main] INFO  PerfTrace - PerfTrace traceId=job_-1, isEnable=false, priority=0
2020-09-23 19:51:31.069 [main] INFO  JobContainer - DataX jobContainer starts job.
2020-09-23 19:51:31.070 [main] INFO  JobContainer - Set jobId = 0
2020-09-23 19:51:31.082 [job-0] INFO  JobContainer - jobContainer starts to do prepare ...
2020-09-23 19:51:31.082 [job-0] INFO  JobContainer - DataX Reader.Job [streamreader] do prepare work .
2020-09-23 19:51:31.083 [job-0] INFO  JobContainer - DataX Writer.Job [streamwriter] do prepare work .
2020-09-23 19:51:31.083 [job-0] INFO  JobContainer - jobContainer starts to do split ...
2020-09-23 19:51:31.083 [job-0] INFO  JobContainer - Job set Channel-Number to 1 channels.
2020-09-23 19:51:31.083 [job-0] INFO  JobContainer - DataX Reader.Job [streamreader] splits to [1] tasks.
2020-09-23 19:51:31.084 [job-0] INFO  JobContainer - DataX Writer.Job [streamwriter] splits to [1] tasks.
2020-09-23 19:51:31.102 [job-0] INFO  JobContainer - jobContainer starts to do schedule ...
2020-09-23 19:51:31.111 [job-0] INFO  JobContainer - Scheduler starts [1] taskGroups.
2020-09-23 19:51:31.117 [taskGroup-0] INFO  TaskGroupContainer - taskGroupId=[0] start [1] channels for [1] tasks.
2020-09-23 19:51:31.119 [taskGroup-0] INFO  Channel - Channel set byte_speed_limit to -1, No bps activated.
2020-09-23 19:51:31.120 [taskGroup-0] INFO  Channel - Channel set record_speed_limit to -1, No tps activated.
2020-09-23 19:51:31.129 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] taskId[0] attemptCount[1] is started
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
DataX	19890604	1989-06-04 00:00:00	true	test
2020-09-23 19:51:31.231 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] taskId[0] is successful, used[103]ms
2020-09-23 19:51:31.232 [taskGroup-0] INFO  TaskGroupContainer - taskGroup[0] completed it's tasks.
2020-09-23 19:51:41.129 [job-0] INFO  StandAloneJobContainerCommunicator - Total 10 records, 260 bytes | Speed 26B/s, 1 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2020-09-23 19:51:41.130 [job-0] INFO  AbstractScheduler - Scheduler accomplished all tasks.
2020-09-23 19:51:41.130 [job-0] INFO  JobContainer - DataX Writer.Job [streamwriter] do post work.
2020-09-23 19:51:41.130 [job-0] INFO  JobContainer - DataX Reader.Job [streamreader] do post work.
2020-09-23 19:51:41.130 [job-0] INFO  JobContainer - DataX jobId [0] completed successfully.
2020-09-23 19:51:41.130 [job-0] INFO  JobContainer - invokeHooks begin
2020-09-23 19:51:41.130 [job-0] INFO  JobContainer - report url not found
2020-09-23 19:51:41.133 [job-0] INFO  JobContainer -
	 [total cpu info] =>
		averageCpu                     | maxDeltaCpu                    | minDeltaCpu
		-1.00%                         | -1.00%                         | -1.00%


	 [total gc info] =>
		 NAME                 | totalGCCount       | maxDeltaGCCount    | minDeltaGCCount    | totalGCTime        | maxDeltaGCTime     | minDeltaGCTime
		 PS MarkSweep         | 0                  | 0                  | 0                  | 0.000s             | 0.000s             | 0.000s
		 PS Scavenge          | 2                  | 2                  | 2                  | 0.006s             | 0.006s             | 0.006s

2020-09-23 19:51:41.133 [job-0] INFO  JobContainer - PerfTrace not enable!
2020-09-23 19:51:41.133 [job-0] INFO  StandAloneJobContainerCommunicator - Total 10 records, 260 bytes | Speed 26B/s, 1 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2020-09-23 19:51:41.134 [job-0] INFO  JobContainer - Total 10 records, 260 bytes | Speed 26B/s, 1 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2020-09-23 19:51:41.134 [job-0] INFO  JobContainer -
任务启动时刻                    : 2020-09-23 19:51:31
任务结束时刻                    : 2020-09-23 19:51:41
任务总计耗时                    :                 10s
任务平均流量                    :               26B/s
记录写入速度                    :              1rec/s
读出记录总数                    :                  10
读写失败总数                    :                   0
```

</details>

更多说明，可以说明文档

## 运行要求

- JDK 1.8+
- Python 2.7+ / Python 3.7+

## 文档

- [在线文档](https://datax.readthedocs.io)
- [项目内文档](docs/src/main/sphinx/index.rst)

## License

This software is free to use under the Apache License [Apache license](/license.txt).
