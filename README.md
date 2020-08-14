# DataX

该项目从阿里的[DataX](https://github.com/alibaba/datax) 而来，经过了精简和改造，说明如下

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

#### writer plugin

1. dbffilewrite

### 部分插件增强功能，罗列如下

1. hdfswriter 增加了对ORC格式的支持
2. hdfswrite 增加了目录覆盖模式
3. hdfswriter 的临时目录位置改动为当前写入目录下的隐藏目录，解决了之前和写入目录平行导致的自动增加分区的问题
4. hdfswriter 在覆盖模式下，改进了文件删除机制，减少了对应表查询为空的时间窗口
5. hbasex11sqlwrite  增加了 Kerberos 支持
6. 尽可能减少了本地jar包的依赖，转为从maven仓库获取
7. 绝大部分依赖包升级到了最新稳定版本，减少了潜在漏洞
8. 不同插件下的相同依赖包做了版本统一

## 详细介绍

请参考：[DataX-Introduction](docs/introduction.md)

## 快速开始

### 编译

```shell
git clone https://github.com/wgzhao/DataX.git
cd DataX
mvn clean package assembly:assembly
```

### 测试

```shell
cd target/datax-<version>/datax-<version>
bin/datax.py job/job.json
```

更多的配置样例可以参考 `job` 目录下的文件

## 支持的数据库类型

| 类型               | 数据源        | Reader(读) | Writer(写) |                                文档                                 |
| ------------------ | ------------- | :--------: | :--------: | :-----------------------------------------------------------------: |
| RDBMS 关系型数据库   | MySQL         |     √      |     √      |       [读](/docs/mysqlreader.md) 、[写](/docs/mysqlwriter.md)       |
|                    | Oracle        |     √      |     √      |      [读](/docs/oraclereader.md) 、[写](/docs/oraclewriter.md)      |
|                    | SQLServer     |     √      |     √      |   [读](/docs/sqlserverreader.md) 、[写](/docs/sqlserverwriter.md)   |
|                    | PostgreSQL    |     √      |     √      |  [读](/docs/postgresqlreader.md) 、[写](/docs/postgresqlwriter.md)  |
|                    | DRDS          |     √      |     √      |        [读](/docs/drdsreader.md) 、[写](/docs/drdswriter.md)        |
|                    | 通用RDBMS)    |     √      |     √      |       [读](/docs/rdbmsreader.md) 、[写](/docs/rdbmswriter.md)       |
| NoSQL数据存储       | DBF           |     √      |     √      |                    [读](/docs/dbffilereader.md)                     | [写](/docs/dbffilewriter.md) |
|                    | Hbase0.94     |     √      |     √      |   [读](/docs/hbase094xreader.md) 、[写](/docs/hbase094xwriter.md)   |
|                    | Hbase1.1      |     √      |     √      |    [读](/docs/hbase11xreader.md) 、[写](/docs/hbase11xwriter.md)    |
|                    | Phoenix4.x    |     √      |     √      | [读](/docs/hbase11xsqlreader.md) 、[写](/docs/hbase11xsqlwriter.md) |
|                    | Phoenix5.x    |     √      |     √      | [读](/docs/hbase20xsqlreader.md) 、[写](/docs/hbase20xsqlwriter.md) |
|                    | MongoDB       |     √      |     √      |       [读](/docs/mongoreader.md) 、[写](/docs/mongowriter.md)       |
|                    | Hive          |     √      |     √      |        [读](/docs/hdfsreader.md) 、[写](/docs/hdfswriter.md)        |
| 无结构化数据存储     | TxtFile       |     √      |     √      |     [读](/docs/txtfilereader.md) 、[写](/docs/txtfilewriter.md)     |
|                    | FTP           |     √      |     √      |         [读](/docs/ftpreader.md) 、[写](/docs/ftpwriter.md)         |
|                    | HDFS          |     √      |     √      |        [读](/docs/hdfsreader.md) 、[写](/docs/hdfswriter.md)        |
|                    | Elasticsearch |            |     √      |                 [写](/docs/elasticsearchwriter.md)                  |
| 时间序列数据库       | OpenTSDB      |     √      |            |                    [读](/docs/opentsdbreader.md)                    |
|                    | TSDB          |            |     √      |                    [写](/docs/tsdbhttpwriter.md)                    |

## License

This software is free to use under the Apache License [Apache license](/license.txt).
