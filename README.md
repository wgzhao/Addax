<p align="center">
    <img alt="DataX Logo" src="https://github.com/wgzhao/DataX/blob/master/docs/images/datax-logo.png?raw=true" width="205" />
</p>
<p align="center">DataX is an open source univeral ETL tool</p>
<p align="center"><a href="https://datax.readthedocs.io">Documentation</a> Detailed description of how to install and deploy and how to use each collection plugin </p>
<p align="center">
   <a href="https://github.com/wgzhao/DataX/workflows/Maven%20Package/badge.svg">
       <img src="https://github.com/wgzhao/DataX/workflows/Maven%20Package/badge.svg" alt="Maven Package" />
   </a>
   <a href="https://datax.readthedocs.io/zh_CN/latest/?badge=latest">
       <img src="https://readthedocs.org/projects/datax/badge/?version=latest" alt="Documentation Status" />
   </a>
</p>

![JetBrains](./jetbrains.png)

This project is supported by [JetBrains](https://jb.gg/OpenSource)

English | [简体中文](README_zh.md)

## current stable version

`3.2.3`

Note: As of `3.2.1`, the package class names have been changed and are therefore no longer compatible with `3.1.x` versions.

The project, originally from Ali's [DataX]((https://github.com/alibaba/datax)), has been streamlined and adapted, as described below

## Description of functional differences

### Removed

Deleted databases that were restricted to Ali internal databases that were not available in 
non-Ali groups and were therefore deleted outright, including:

- ADS
- DRDS
- OCS
- ODPS
- OSS
- OTS

### Added

Added some plug-ins, which currently include

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

### Some plug-in enhancements are listed below

#### rdbms-ralative plugins

add support for almost basic data type, and some complex data type.

#### hdfswriter

1. Add support for Decimal data type.
2. Add support for writing Parquet files.
3. Add support for writing with the overwrite mode.
4. Add support for more compression algorithm.
5. The temporary directory location is changed to a hidden directory under the current write directory, which solves the problem of automatic partition increase caused by the previous parallelism with the write directory.
6. In overwrite mode, the file deletion mechanism has been improved to reduce the time window when the corresponding table query is empty

#### hdfsreader

1. Add support for reading Parquet files.
2. Add support for more compression algorithm.

#### hbasex11sqlwrite

1. Add support for Kerberos authentication.

#### oraclewriter

1. Add support for `merge into` statement.

#### postgresqlwriter

1. Add support for `insert into ... on conflict` statement.

#### rdbmsreader/rdbmswriter

1. Add support TDH Inceptor, Trino query engine

## Supported databases

| database/filesystem | reader | writer | plugin(reader/writer)                   | memo                                                                                       |
| ------------------- | ------ | ------ | --------------------------------------- | ------------------------------------------------------------------------------------------ |
| Cassander           | YES    | YES    | cassandrareader/cassandrawriter         |                                                                                            |
| ClickHouse          | YES    | YES    | clickhousereader/clickhousewriter       |                                                                                            |
| DB2                 | YES    | YES    | rbdmsreader/rdbmswriter                 | not fully tested                                                                           |
| DBF                 | YES    | YES    | dbffilereader/dbffilewriter             |                                                                                            |
| ElasticSearch       | YES    | YES    | elasticsearchreader/elasticsearchwriter | originally from [@Kestrong](https://github.com/Kestrong/datax-elasticsearch)               |
| FTP                 | YES    | YES    | ftpreader/ftpwriter                     |                                                                                            |
| HBase 1.x           | YES    | YES    | hbase11xreader/hbase11xwriter           | use HBASE API                                                                              |
| HBase 1.x           | YES    | YES    | hbase11xsqlreader/hbase11xsqlwriter     | use Phoenix[Phoenix](https://phoenix.apache.org)                                           |
| HBase 2.x           | YES    | NO     | hbase20xreader                          | use HBase API                                                                              |
| HBase 2.x           | YES    | YES    | hbase20xsqlreader/hbase20xsqlwriter     | 通过[Phoenix](https://phoenix.apache.org)操作HBase                                         |
| HDFS                | YES    | YES    | hdfsreader/hdfswriter                   | support HDFS 2.0 or later                                                                  |
| HTTP                | YES    | NO     | httpreader                              | support RestFul API                                                                        |
| Greenplum           | YES    | YES    | postgresqlreader/greenplumwriter        |                                                                                            |
| InfluxDB            | YES    | YES    | influxdbreader/influxdbwriter           | ONLY support  InfluxDB 1.x                                                                 |
| json                | YES    | NO     | jsonfilereader                          |                                                                                            |
| kudu                | YES    | YES    | kudureader/kuduwriter                   |                                                                                            |
| MongoDB             | YES    | YES    | mongodbreader/mongodbwriter             |                                                                                            |
| MySQL/MariaDB       | YES    | YES    | mysqlreader/mysqlwriter                 |                                                                                            |
| Oracle              | YES    | YES    | oraclereader/oraclewriter               |                                                                                            |
| PostgreSQL          | YES    | YES    | postgresqlreader/postgresqlwriter       |                                                                                            |
| Trino               | YES    | YES    | rdbmsreader/rdbmswriter                 | [trino( formerly PrestoSQL)](https://trino.io)                                             |
| Redis               | YES    | YES    | redisreader/rediswriter                 |                                                                                            |
| SQL Server          | YES    | YES    | sqlserverreader/sqlserverwriter         |                                                                                            |
| TDengine            | YES    | YES    | tdenginereader/tdenginewriter           | [TDengine](https://www.taosdata.com/cn/)                                                   |
| TDH Inceptor2       | YES    | YES    | rdbmsreader/rdbmswriter                 | [Transwarp TDH](http://transwarp.cn/transwarp/product-TDH.html?categoryId=18) 5.1 or later |
| TEXT                | YES    | YES    | textfilereader/textfilewriter           |                                                                                            |

## quick started

### Do not want to compile?

If you are too lazy to compile or cannot compile because of your environment, you can download the corresponding version from the following link

| version | download                                                   | md5                              |
| ------- | ---------------------------------------------------------- | -------------------------------- |
| 3.2.3   | https://pan.baidu.com/s/1ajjnSittf6u7rjXhJ7_3Aw code: qxry | ad47b0d840bf21de1668b9310a9782cf |
| 3.2.2   | https://pan.baidu.com/s/1TQyaERnIk9EQRDULfQE69w code: jh31 | b04d2563adb36457b85e48c318757ea3 |
| 3.2.1   | https://pan.baidu.com/s/1as6sL09HlxAN8b2pZ1DttQ code: hwgx | ecda4a961b032c75718502caf54246a8 |
| 3.1.9   | https://pan.baidu.com/s/1GYpehEvB-W3qnqilhskXFw code: q4wv | 48c4104294cd9bb0c749efc50b32b4dd |
| 3.1.8   | https://pan.baidu.com/s/1jv-tb-11grYaUnsgnEhDzw code: 2dnf | ef110ae1ea31e1761dc25d6930300485 |
| 3.1.7   | https://pan.baidu.com/s/1CE5I8V5TNptdOp6GLid3Jg code: v5u3 | fecca6c4a32f2bf7246fdef8bc2912fe |
| 3.1.6   | https://pan.baidu.com/s/1Ldg10E3qWkbUT44rkH19og code: 4av4 | f6aea7e0ce4b9ec83554e9c6d6ab3cb6 |
| 3.1.5   | https://pan.baidu.com/s/1yY_lJqulE6hKqktoQbbGmQ code: 2r4p | 9ae27c1c434a097f67a17bb704f70731 |
| 3.1.4   | https://pan.baidu.com/s/1_plsvzD_GrWN-HffPBtz-g code: kpjn | 7aca526fe7f6f0f54dc467f6ca1647b1 |
| 3.1.2   | https://pan.baidu.com/s/1zFqv8E6iJX549zdSZDQgiQ code: 7jdk | 3674711fc9b68fad3086f3c8526a3427 |
| 3.1.1   | https://pan.baidu.com/s/1GwmFA7-hPkd6GKiZEvUKXg code: 1inn | 0fa4e7902420704b2e814fef098f40ae |

**Note**: Starting from version `3.2.3`, in order to reduce the installation package size, the compiled package only includes `streamreader` and `streamwriter` plug-ins, 
other plug-ins need to be downloaded separately, the download shared directory list is as follows.

### plugins download

| version | download link                                              |
| ------- | ---------------------------------------------------------- |
| 3.2.3   | https://pan.baidu.com/s/1g4z3Pqc_BxKstkiYjWXopQ code: 2fip |

### compile and package

```shell
git clone https://github.com/wgzhao/datax.git DataX
cd DataX
mvn clean package
mvn package assembly:single
```

If you want compile doc, you can execute the following instructions.

```shell
cd docs
mvn clean package
```

After successful compilation and packaging, a `datax-<version>` folder will be created in the `target/datax` directory of the project directory, where `<version` indicates the version.

### begin your first job

The `job` subdirectory contains many sample jobs, of which `job.json` can be used as a smoke-out test and executed as follows

```shell
cd target/datax/datax-<version>
python bin/datax.py job/job.json
```

The output of the above command is roughly as follows.

<details>
<summary>Click to expand</summary>

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


## runtime requirements

- JDK 1.8+
- Python 2.7+ / Python 3.7+

## documentation

- [online](https://datax.readthedocs.io)
- [project](docs/src/main/sphinx/index.rst)

## License

This software is free to use under the Apache License [Apache license](/LICENSE).
