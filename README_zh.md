<p align="center">
    <img alt="Addax Logo" src="https://github.com/wgzhao/Addax/blob/master/docs/images/logo.png?raw=true" width="205" />
</p>
<p align="center">Addax 是一个支持主流数据库的通用数据采集工具</p>
<p align="center"><a href="https://wgzhao.github.io/Addax">使用文档</a> 详细描述了如何安装部署和每个采集插件的使用方法 </p>
<p align="center">
   <a href="https://github.com/wgzhao/addax/release">
      <img src="https://img.shields.io/github/release/wgzhao/addax.svg" alt="release version"/>
    </a>
   <a href="https://github.com/wgzhao/Addax/workflows/Maven%20Package/badge.svg">
       <img src="https://github.com/wgzhao/Addax/workflows/Maven%20Package/badge.svg" alt="Maven Package" />
   </a>
</p>

该项目原始代码来自阿里开源的 [DataX](https://github.com/alibaba/datax) ，但经过了大幅修改，详细情况可参考[与DataX的主要区别](difference.md)

## 支持的数据库一览表

![supported databases](docs/images/supported_databases.png)

| 数据库或文件系统 | 读取 | 写入   | 插件名称(reader/writer)                 | 备注                             |
| ---------------- | ---- | ------ | --------------------------------------- | -------------------------------- |
| Cassandra        | 支持 | 支持   | cassandrareader/cassandrawriter         |                                  |
| ClickHouse       | 支持 | 支持   | clickhousereader/clickhousewriter       |                                  |
| DB2              | 支持 | 支持   | rbdmsreader/rdbmswriter                 | 理论上支持，但未实际测试         |
| DBF              | 支持 | 支持   | dbfreader/dbfwriter                     |                                  |
| ElasticSearch    | 支持 | 支持   | elasticsearchreader/elasticsearchwriter | 原始代码来自[@Kestrong][1]       |
| Excel            | 支持 | 不支持 | excelreader/excelwriter                 |                                  |
| FTP              | 支持 | 支持   | ftpreader/ftpwriter                     |                                  |
| HBase 1.x        | 支持 | 支持   | hbase11xreader/hbase11xwriter           | 直接操作HBase                    |
| HBase 1.x        | 支持 | 支持   | hbase11xsqlreader/hbase11xsqlwriter     | 通过[Phoenix][2] 操作HBase       |
| HBase 2.x        | 支持 | 不支持 | hbase20xreader                          | 直接操作HBase                    |
| HBase 2.x        | 支持 | 支持   | hbase20xsqlreader/hbase20xsqlwriter     | 通过[Phoenix][2] 操作HBase       |
| HDFS             | 支持 | 支持   | hdfsreader/hdfswriter                   | HDFS 2.x 以上版本                |
| HTTP             | 支持 | 不支持 | httpreader                              | 仅支持返回值为JSON类型的接口     |
| Greenplum        | 支持 | 支持   | postgresqlreader/greenplumwriter        |                                  |
| InfluxDB         | 支持 | 支持   | influxdbreader/influxdbwriter           | 仅支持1.x版本，2.0及以上暂不支持 |
| json             | 支持 | 不支持 | jsonfilereader                          |                                  |
| kudu             | 支持 | 支持   | kudureader/kuduwriter                   | 通过原生接口，计划更新Impala连接 |
| MongoDB          | 支持 | 支持   | mongodbreader/mongodbwriter             |                                  |
| MySQL/MariaDB    | 支持 | 支持   | mysqlreader/mysqlwriter                 |                                  |
| Oracle           | 支持 | 支持   | oraclereader/oraclewriter               |                                  |
| PostgreSQL       | 支持 | 支持   | postgresqlreader/postgresqlwriter       |                                  |
| PrestoSQL        | 支持 | 支持   | rdbmsreader/rdbmswriter                 | [trino][3] 310以上               |
| Redis            | 支持 | 支持   | redisreader/rediswriter                 |                                  |
| SQLite           | 支持 | 支持   | sqlitereader/sqlitewriter               |                                  |
| SQL Server       | 支持 | 支持   | sqlserverreader/sqlserverwriter         |                                  |
| TDengine         | 支持 | 支持   | tdenginereader/tdenginewriter           | 支持 [TDengine][4] 数据库读写    |
| TDH Inceptor2    | 支持 | 支持   | rdbmsreader/rdbmswriter                 | [星环 TDH][5] 5.1以上版本        |
| TEXT             | 支持 | 支持   | textfilereader/textfilewriter           |                                  |

[1]: https://github.com/Kestrong/datax-elasticsearch
[2]: https://phoenix.apache.org
[3]: https://trino.io
[4]: https://www.taosdata.com/cn/
[5]: http://transwarp.cn/

## 快速开始

### 使用 Docker 镜像

```shell
docker pull wgzhao/addax:latest
docker run -ti --rm --name addax wgzhao/addax:latest /opt/addax/bin/addax.sh /opt/addax/job/job.json
```

### 不想编译

如果你懒得编译或者因为环境无法编译，可以从以下链接下载对应的版本

| 版本  | 连接地址                                                     | md5值                            |
| ----- | ------------------------------------------------------------ | -------------------------------- |
| 4.0.3 | https://www.aliyundrive.com/s/8CRAfMBbwfm                    | 19766c2577b46bd5b22d63a502f5f5dd |
| 4.0.2 | https://www.aliyundrive.com/s/U5uotY7vVAY                    | cd3a3d6d0c79cbd3bcd259ebb47acbc5 |
| 4.0.1 | https://www.aliyundrive.com/s/BwbUJr21baH                    | 8f1963e8ce5e5f880a29a503399413a6 |
| 4.0.0 | https://pan.baidu.com/s/1qmV6ed3CYpACIp29JCIDgQ 提取码: 559q | b9b759da228f3bc656965d20357dcb2a |
| 3.2.5 | https://pan.baidu.com/s/14_MnbtRUtJlvQh8tTKv6fg 提取码: 1jdr | 43ddd0186ccbaf1f1bfee0aac22da935 |
| 3.2.4 | https://pan.baidu.com/s/1VaOlAOTqGX4WwRtI5ewPeg 提取码: i127 | 2d16125385b88405481e12bf4a8fd715 |
| 3.2.3 | https://pan.baidu.com/s/1ajjnSittf6u7rjXhJ7_3Aw 提取码: qxry | ad47b0d840bf21de1668b9310a9782cf |
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

注：

1. 从4.0.1版本开始，上传的二进制文件从百度网盘切换到了阿里云网盘，下载速度应该会有很大的提升
2. 从 3.2.3 版本开始，为了减少安装包大小，编译好的压缩包仅包括 `streamreader` 和 `streamwriter` 两个插件，其他插件则需要单独下载，下载共享目录列表如下：
3. 因为阿里云盘暂时不支持压缩文件的分享，所以上述提供下载的二进制版本文件我添加了 `.jgp` 后缀，下载后可以删除这个后缀。

预编译插件下载地址一览表

| 版本  | 插件下载地址                                                 |
| ----- | ------------------------------------------------------------ |
| 4.0.0 | https://pan.baidu.com/s/1gLWiw2I7W_4-KBiA1CCg2g 提取码: hxag |
| 3.2.5 | https://pan.baidu.com/s/1VMqPAYeL_kirCjOVAdvoAg 提取码: hda9 |
| 3.2.4 | https://pan.baidu.com/s/1gPJlJh66bGQUSUR-2mNOQw 提取码: 7c4j |
| 3.2.3 | https://pan.baidu.com/s/1g4z3Pqc_BxKstkiYjWXopQ 提取码: 2fip |

### 编译及打包

```shell
git clone https://github.com/wgzhao/addax.git addax
cd addax
mvn clean package -pl '!:addax-docs'
mvn package assembly:single
```

如果需要编译文档，请执行下面的命令

```shell
cd docs
mvn clean package
```

编译打包成功后，会在项目目录的`target/addax` 目录下创建一个 `addax-<version>`的 文件夹，其中 `<version>` 表示版本。

### 开始第一个任务

`job` 子目录包含了大量的任务样本，其中 `job.json` 可以作为冒烟测试，执行如下

```shell
cd target/addax/addax-<version>
bin/addax.sh job/job.json
```

上述命令的输出大致如下：
<details>
<summary>点击展开</summary>

```shell
$bin/addax.sh job/job.json

  ___      _     _
 / _ \    | |   | |
/ /_\ \ __| | __| | __ ___  __
|  _  |/ _` |/ _` |/ _` \ \/ /
| | | | (_| | (_| | (_| |>  <
\_| |_/\__,_|\__,_|\__,_/_/\_\

:: Addax version ::    (v4.0.3)

2021-09-16 11:03:20.328 [        main] INFO  VMInfo               - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2021-09-16 11:03:20.347 [        main] INFO  Engine               -
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

2021-09-16 11:03:20.367 [        main] INFO  PerfTrace            - PerfTrace traceId=job_-1, isEnable=false, priority=0
2021-09-16 11:03:20.367 [        main] INFO  JobContainer         - Addax jobContainer starts job.
2021-09-16 11:03:20.368 [        main] INFO  JobContainer         - Set jobId = 0
2021-09-16 11:03:20.382 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] do prepare work .
2021-09-16 11:03:20.382 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do prepare work .
2021-09-16 11:03:20.383 [       job-0] INFO  JobContainer         - Job set Channel-Number to 1 channels.
2021-09-16 11:03:20.383 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] splits to [1] tasks.
2021-09-16 11:03:20.383 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] splits to [1] tasks.
2021-09-16 11:03:20.405 [       job-0] INFO  JobContainer         - Scheduler starts [1] taskGroups.
2021-09-16 11:03:20.412 [ taskGroup-0] INFO  TaskGroupContainer   - taskGroupId=[0] start [1] channels for [1] tasks.
2021-09-16 11:03:20.415 [ taskGroup-0] INFO  Channel              - Channel set byte_speed_limit to -1, No bps activated.
2021-09-16 11:03:20.415 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
addax	19890604	1989-06-04 00:00:00	true	test
2021-09-16 11:03:23.428 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-09-16 11:03:23.428 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do post work.
2021-09-16 11:03:23.428 [       job-0] INFO  JobContainer         - Addax Reader.Job [streamreader] do post work.
2021-09-16 11:03:23.430 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-09-16 11:03:23.431 [       job-0] INFO  StandAloneJobContainerCommunicator - Total 10 records, 260 bytes | Speed 86B/s, 3 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2021-09-16 11:03:23.432 [       job-0] INFO  JobContainer         -
任务启动时刻                    : 2021-09-16 11:03:20
任务结束时刻                    : 2021-09-16 11:03:23
任务总计耗时                    :                  3s
任务平均流量                    :               86B/s
记录写入速度                    :              3rec/s
读出记录总数                    :                  10
读写失败总数                    :                   0
```

</details>

[这里](core/src/main/job) 以及[文档中心](docs/assets/jobs) 提供了大量作业配置样例

## 运行要求

- JDK 1.8+
- Python 2.7+ / Python 3.7+ (Windows)

## 文档

- [在线文档](https://addax.readthedocs.io)
- [项目内文档](docs/src/main/sphinx/index.rst)

## 代码风格

建议使用 IntelliJ 作为开发 IDE。项目的代码风格模板可以在[codestyle](https://github.com/airlift/codestyle)资源库中找到，还有我们的一般编程和Java指南。除了这些之外，你还应该遵守以下几点。

* 在文档源文件中按字母顺序排列章节（包括目录文件和其他常规文档文件）。一般来说，如果周围的代码中已经存在这样的排序，就按字母顺序排列方法/变量/部分。
* 在适当的时候，使用Java 8的流API。然而，请注意，流的实现并不是很好，所以避免在内循环或其他对性能敏感的部分使用它。
* 在抛出异常时对错误进行分类。例如，AddaxException需要一个错误代码作为参数，`AddaxException(RdbmsReaderErrorCode)`。这种分类可以让你生成报告，这样你就可以监控各种故障的频率。
* 确保所有文件都有适当的许可证头；你可以通过运行`mvn license:format`生成许可证。
* 考虑使用字符串格式化（使用Java`Formatter`类的printf风格格式化）：`format("Session property %s is invalid: %s", name, value)`（注意，`format()`应该总是静态导入）。有时，如果你只需要附加一些东西，可以考虑使用`+`运算符。
* 除了琐碎的表达式，避免使用三元操作符。
* 如果有涵盖你的情况的断言，就使用Airlift的`Assertions`类，而不是手工写断言。随着时间的推移，我们可能会转移到更流畅的断言，如AssertJ。
* 在编写Git提交信息时，请遵循这些[指南]（https://chris.beams.io/posts/git-commit/）。

## 版本兼容性说明

- 从 `4.0.0` 版本开始，启用新的项目名称 `Addax`, 因此它和以前的版本均不兼容  
- 从 `3.2.1` 版本开始，包类名已经更改，因此不再兼容 `3.1.x` 版本

## 代码授权许可

该软件在遵守 [Apache license](/license.txt) 下可自由免费使用

## 赞助

![JetBrains](./jetbrains.png)

该项目开发工具由 [JetBrains](https://jb.gg/OpenSource) 提供赞助
