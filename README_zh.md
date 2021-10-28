<p align="center">
    <img alt="Addax Logo" src="https://github.com/wgzhao/Addax/blob/master/docs/images/logo.png?raw=true" width="205" />
</p>
<p align="center">Addax 是一个支持主流数据库的通用数据采集工具</p>
<p align="center"><a href="https://wgzhao.github.io/Addax">使用文档</a> 详细描述了如何安装部署和每个采集插件的使用方法 </p>
<p align="center">
   <a href="https://github.com/wgzhao/Addax/releases">
      <img src="https://img.shields.io/github/release/wgzhao/addax.svg" alt="release version"/>
    </a>
   <a href="https://github.com/wgzhao/Addax/workflows/Maven%20Package/badge.svg">
       <img src="https://github.com/wgzhao/Addax/workflows/Maven%20Package/badge.svg" alt="Maven Package" />
   </a>
</p>

该项目原始代码来自阿里开源的 [DataX](https://github.com/alibaba/datax) ，但经过了大幅修改，详细情况可参考[与DataX的主要区别](difference.md)

## 支持的数据库一览表

Addax 支持超过 20 种[关系型和非关系型数据库](support_data_sources.md)，通过简单的配置，还可以快速增加更多的数据源支持。 

![supported databases](docs/images/supported_databases.png)

## 快速开始

### 使用 Docker 镜像

```shell
docker pull wgzhao/addax:latest
docker run -ti --rm --name addax wgzhao/addax:latest /opt/addax/bin/addax.sh /opt/addax/job/job.json
```

### 不想编译

你可以直接从[发布页面](https://github.com/wgzhao/Addax/releases)下载需要的版本可

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

- [在线文档](https://wgzhao.github.io/Addax/)
- [项目内文档](docs/index.md)

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
