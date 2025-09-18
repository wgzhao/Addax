<p align="center">
    <img alt="Addax Logo" src="https://github.com/wgzhao/Addax/blob/master/docs/images/logo.svg?raw=true" width="205" />
</p>
<p align="center">Addax 是一个支持主流数据库的通用数据采集工具</p>
<p align="center"><a href="https://wgzhao.github.io/Addax">使用文档</a> 详细描述了如何安装使用，针对每个插件都有详细的说明和样例配置文档 </p>
<p align="center">
   <a href="https://github.com/wgzhao/Addax/releases">
      <img src="https://img.shields.io/github/release/wgzhao/addax.svg" alt="release version"/>
    </a>
   <a href="https://github.com/wgzhao/Addax/workflows/Maven%20Package/badge.svg">
       <img src="https://github.com/wgzhao/Addax/workflows/Maven%20Package/badge.svg" alt="Maven Package" />
   </a>
</p>

该项目原始代码来自阿里开源的 [DataX](https://github.com/alibaba/datax) ，在此基础上经过了大量的改进，并提供了更多的读写插件，详细情况可参考[与DataX的主要区别](difference.md)

## 支持的数据库一览表

Addax 支持超过 20 种[关系型和非关系型数据库](support_data_sources.md)，通过简单的配置，还可以快速增加更多的数据源支持。

<table>
<tr>
<td><img src="./docs/images/logos/cassandra.svg" height="50px" alt="Cassandra" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/clickhouse.svg" height="50px" alt="Clickhouse" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/databend.svg" height="50px" alt="DataBend" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/db2.svg" height="50px" alt="IMB DB2" style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/dbase.svg" height="50px" alt="dBase" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/doris.svg"  height="50px" alt="Doris" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/elasticsearch.svg" height="50px" alt="Elasticsearch" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/excel.svg" height="50px" alt="Excel" style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/greenplum.svg" height="50px" alt="Greenplum" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/hbase.svg" height="50px" alt="Apache HBase" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/hive.svg" height="50px" alt="Hive" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/influxdata.svg" height="50px" alt="InfluxDB" style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/kafka.svg" height="50px" alt="Kafka" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/kudu.svg" height="50px" alt="Kudu" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/minio.svg" height="50px" alt="MinIO" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/mongodb.svg" height="50px" alt="MongoDB" style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/mysql.svg" height="50px" alt="MySQL" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/oracle.svg" height="50px" alt="Oracle" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/phoenix.svg" height="50px" alt="Phoenix" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/postgresql.svg" height="50px" alt="PostgreSQL" style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/presto.svg" height="50px" alt="Presto" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/redis.svg" height="50px" alt="Redis" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/s3.svg" height="50px" alt="Amazon S3" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/sqlite.svg" height="50px" alt="SQLite" style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/sqlserver.svg" height="50px" alt="SQLServer" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/starrocks.svg" height="50px" alt="Starrocks" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/sybase.svg" height="50px" alt="Sybase" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/tdengine.svg" height="50px" alt="TDengine"  style="border: 1px solid #ddd;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/trino.svg" height="50px" alt="Trino" style="border: 1px solid #ddd;"></td>
<td><img src="./docs/images/logos/access.svg" height="50px" alt="Access" style="border: 1px solid #add;"></td>
<td><img src="./docs/images/logos/sap.svg" height="50px" alt="SAP HANA" style="border: 1px solid #add;"></td>
<td><img src="./docs/images/logos/paimon.svg" height="50px" alt="Paimon" style="border: 1px solid #add;"></td>
</tr>
<tr>
<td><img src="./docs/images/logos/iceberg.svg" height="50px" alt="Iceberg" style="border: 1px solid #add;"></td>
</tr>
</table>

## 快速开始

### 使用 Docker 镜像

```shell
docker pull quay.io/wgzhao/addax:latest
docker run -ti --rm --name addax \
  quay.io/wgzhao/addax:latest \
  /opt/addax/bin/addax.sh /opt/addax/job/job.json
```

### 使用一键安装脚本

```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/wgzhao/Addax/master/install.sh)"
```
上述脚本会将 Addax 安装到预设的目录( 对于 macOS Intel 而言，目录为 `/usr/local`,  Apple Silicon 以及 Linux 系统则为 `/opt/addax`)

### 编译及打包

目前编译需要 JDK 17 版本

```shell
git clone https://github.com/wgzhao/addax.git addax
cd addax
export MAVEN_OPTS="-DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true"
mvn clean package 
mvn package -Pdistribution
```

编译打包成功后，会在项目目录的`target` 目录下创建一个 `addax-<version>`的 文件夹，其中 `<version>` 表示版本。

### 开始第一个任务

`job` 子目录包含了大量的任务样本，其中 `job.json` 可以作为冒烟测试，执行如下

```shell
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

:: Addax version ::    (v4.0.13-SNAPSHOT)

2023-05-14 11:43:38.040 [        main] INFO  VMInfo               - VMInfo# operatingSystem class => sun.management.OperatingSystemImpl
2023-05-14 11:43:38.062 [        main] INFO  Engine               -
{
	"setting":{
		"speed":{
			"byte":-1,
			"channel":1,
			"record":-1
		}
	},
	"content":{
		"reader":{
			"name":"streamreader",
			"parameter":{
				"sliceRecordCount":10,
				"column":[
					{
						"value":"addax",
						"type":"string"
					},
					{
						"value":19890604,
						"type":"long"
					},
					{
						"value":"1989-06-04 11:22:33 123456",
						"type":"date",
						"dateFormat":"yyyy-MM-dd HH:mm:ss SSSSSS"
					},
					{
						"value":true,
						"type":"bool"
					},
					{
						"value":"test",
						"type":"bytes"
					}
				]
			}
		},
		"writer":{
			"name":"streamwriter",
			"parameter":{
				"print":true,
				"encoding":"UTF-8"
			}
		}
	}
}

2023-05-14 11:43:38.092 [        main] INFO  JobContainer         - The jobContainer begins to process the job.
2023-05-14 11:43:38.107 [       job-0] INFO  JobContainer         - The Reader.Job [streamreader] perform prepare work .
2023-05-14 11:43:38.107 [       job-0] INFO  JobContainer         - The Writer.Job [streamwriter] perform prepare work .
2023-05-14 11:43:38.108 [       job-0] INFO  JobContainer         - Job set Channel-Number to 1 channel(s).
2023-05-14 11:43:38.108 [       job-0] INFO  JobContainer         - The Reader.Job [streamreader] is divided into [1] task(s).
2023-05-14 11:43:38.108 [       job-0] INFO  JobContainer         - The Writer.Job [streamwriter] is divided into [1] task(s).
2023-05-14 11:43:38.130 [       job-0] INFO  JobContainer         - The Scheduler launches [1] taskGroup(s).
2023-05-14 11:43:38.138 [ taskGroup-0] INFO  TaskGroupContainer   - The taskGroupId=[0] started [1] channels for [1] tasks.
2023-05-14 11:43:38.141 [ taskGroup-0] INFO  Channel              - The Channel set byte_speed_limit to -1, No bps activated.
2023-05-14 11:43:38.141 [ taskGroup-0] INFO  Channel              - The Channel set record_speed_limit to -1, No tps activated.
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
addax  19890604	1989-06-04 11:24:36	true	test
2023-05-14 11:43:41.157 [       job-0] INFO  AbstractScheduler    - The scheduler has completed all tasks.
2023-05-14 11:43:41.158 [       job-0] INFO  JobContainer         - The Writer.Job [streamwriter] perform post work.
2023-05-14 11:43:41.159 [       job-0] INFO  JobContainer         - The Reader.Job [streamreader] perform post work.
2023-05-14 11:43:41.162 [       job-0] INFO  StandAloneJobContainerCommunicator - Total 10 records, 260 bytes | Speed 86B/s, 3 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2023-05-14 11:43:41.596 [       job-0] INFO  JobContainer         -
Job start  at             : 2023-05-14 11:43:38
Job end    at             : 2023-05-14 11:43:41
Job took secs             :                  3ss
Average   bps             :               86B/s
Average   rps             :              3rec/s
Number of rec             :                  10
Failed record             :                   0
```

</details>

[这里](core/src/main/job) 以及[文档中心](docs/assets/jobs) 提供了大量作业配置样例

## 运行要求

- JDK 17 or later
- Python 2.7+ / Python 3.7+ (Windows)

## 文档

- [在线文档](https://wgzhao.github.io/Addax/)
- [项目内文档](docs/index.md)

## 相关项目

- [addax-admin](https://github.com/wgzhao/addax-admin) - 基于web的管理工具，用于管理基于Addax的数据采集任务
- [addax-ui](https://github.com/wgzhao/addax-ui) - addax-admin的前端实现

## 代码风格

建议使用 IntelliJ 作为开发 IDE。项目的代码风格模板可以在[codestyle](https://github.com/airlift/codestyle)资源库中找到，还有我们的一般编程和Java指南。除了这些之外，你还应该遵守以下几点。

* 在文档源文件中按字母顺序排列章节（包括目录文件和其他常规文档文件）。一般来说，如果周围的代码中已经存在这样的排序，就按字母顺序排列方法/变量/部分。
* 在适当的时候，使用Java 8的流API。然而，请注意，流的实现并不是很好，所以避免在内循环或其他对性能敏感的部分使用它。
* 在抛出异常时对错误进行分类。例如，AddaxException 使用错误代码和错误消息作为参数，`AddaxException(REQUIRE_VALUE, "lack of required item")`。这种分类可以让你生成报告，这样你就可以监控各种故障。
* 确保所有文件都有适当的许可证头；你可以通过运行`mvn license:format`生成许可证。
* 考虑使用字符串格式化（使用Java`Formatter`类的printf风格）：`format("Session property %s is invalid: %s", name, value)`（注意，`format()`应该总是静态导入）。有时，如果你只需要附加一些东西，可以考虑使用`+`运算符。
* 除了琐碎的表达式，避免使用三元操作符。
* 如果有涵盖你的情况的断言，就使用Airlift的`Assertions`类，而不是手工写断言。
* 在编写Git提交信息时，请遵循这些[指南]（https://chris.beams.io/posts/git-commit/）。

## 版本命名规范

本项目遵循 [语义化版本规则](https://semver.org/lang/zh-CN/) 管理版本号，格式为 `x.y.z`，每个部分含义如下：

- **z（补丁版本）**:
  - 修复问题、性能优化等小改动，不影响现有功能的兼容性。
  - 示例: `1.2.3 → 1.2.4`

- **y（次要版本）**:
  - 添加新功能、模块调整，可能打破部分兼容性。
  - 示例: `1.2.3 → 1.3.0`

- **x（主要版本）**:
  - 引入重大变更或全新功能，大概率与前版本不兼容。
  - 示例: `1.3.0 → 2.0.0`

## Star History

## Star History

<a href="https://www.star-history.com/#wgzhao/Addax&Date">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=wgzhao/Addax&type=Date&theme=dark" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=wgzhao/Addax&type=Date" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=wgzhao/Addax&type=Date" />
 </picture>
</a>

## 代码授权许可

该软件在遵守 [Apache license](/license.txt) 下可自由免费使用

## 特别感谢

特别感谢 [JetBrains](https://jb.gg/OpenSource) 对本项目提供开发工具赞助
