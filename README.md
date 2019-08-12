![Datax-logo](https://github.com/wgzhao/DataX/blob/toad/images/DataX-logo.jpg)


# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、DRDS 等各种异构数据源之间高效的数据同步功能。



# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/wgzhao/DataX/blob/toad/introduction.md)



# Quick Start

##### Download [DataX下载地址](http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz)

##### 请点击：[Quick Start](https://github.com/wgzhao/DataX/blob/toad/userGuid.md)



# Support Data Channels 

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入，目前支持数据如下图，详情请点击：[DataX数据源参考指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)

| 类型           | 数据源        | Reader(读) | Writer(写) |文档|
| ------------ | ---------- | :-------: | :-------: |:-------: |
| RDBMS 关系型数据库 | MySQL      |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/mysqlreader/doc/mysqlreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/mysqlwriter/doc/mysqlwriter.md)|
|              | Oracle     |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/oraclereader/doc/oraclereader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/oraclewriter/doc/oraclewriter.md)|
|              | SQLServer  |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/sqlserverreader/doc/sqlserverreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/sqlserverwriter/doc/sqlserverwriter.md)|
|              | PostgreSQL |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/postgresqlreader/doc/postgresqlreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/postgresqlwriter/doc/postgresqlwriter.md)|
|              | DRDS       |      √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/drdsreader/doc/drdsreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/drdswriter/doc/drdswriter.md)|
|              | 通用RDBMS(支持所有关系型数据库)         |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/rdbmsreader/doc/rdbmsreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/rdbmswriter/doc/rdbmswriter.md)|
|              | DBF        |   √        |      √    |[读](https://github.com/wgzhao/DataX/blob/toad/dbffilereader/doc/dbffilereader.md) | [写](https://github.com/wgzhao/DataX/blob/toad/dbffilewriter/doc/dbffilewriter.md) |
| 阿里云数仓数据存储    | ODPS       |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/odpsreader/doc/odpsreader.md) | [写](https://github.com/wgzhao/DataX/blob/toad/odpswriter/doc/odpswriter.md)|
|              | ADS        |           |     √     |[写](https://github.com/wgzhao/DataX/blob/toad/adswriter/doc/adswriter.md)|
|              | OSS        |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/ossreader/doc/ossreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/osswriter/doc/osswriter.md)|
|              | OCS        |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/ocsreader/doc/ocsreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/ocswriter/doc/ocswriter.md)|
| NoSQL数据存储    | OTS        |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/otsreader/doc/otsreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/otswriter/doc/otswriter.md)|
|              | Hbase0.94  |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/hbase094xreader/doc/hbase094xreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/hbase094xwriter/doc/hbase094xwriter.md)|
|              | Hbase1.1   |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/hbase11xreader/doc/hbase11xreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/hbase11xwriter/doc/hbase11xwriter.md)|
|              | Phoenix4.x   |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/hbase11xsqlreader/doc/hbase11xsqlreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/hbase11xsqlwriter/doc/hbase11xsqlwriter.md)|
|              | Phoenix5.x   |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/hbase20xsqlreader/doc/hbase20xsqlreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/hbase20xsqlwriter/doc/hbase20xsqlwriter.md)|
|              | MongoDB    |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/mongoreader/doc/mongoreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/mongowriter/doc/mongowriter.md)|
|              | Hive       |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/hdfsreader/doc/hdfsreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/hdfswriter/doc/hdfswriter.md)|
| 无结构化数据存储     | TxtFile    |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/txtfilereader/doc/txtfilereader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/txtfilewriter/doc/txtfilewriter.md)|
|              | FTP        |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/ftpreader/doc/ftpreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/ftpwriter/doc/ftpwriter.md)|
|              | HDFS       |     √     |     √     |[读](https://github.com/wgzhao/DataX/blob/toad/hdfsreader/doc/hdfsreader.md) 、[写](https://github.com/wgzhao/DataX/blob/toad/hdfswriter/doc/hdfswriter.md)|
|              | Elasticsearch       |         |     √     |[写](https://github.com/wgzhao/DataX/blob/toad/elasticsearchwriter/doc/elasticsearchwriter.md)|
| 时间序列数据库 | OpenTSDB | √ |  |[读](https://github.com/wgzhao/DataX/blob/toad/opentsdbreader/doc/opentsdbreader.md)|
|  | TSDB | | √ |[写](https://github.com/wgzhao/DataX/blob/toad/tsdbwriter/doc/tsdbhttpwriter.md)|

# License

This software is free to use under the Apache License [Apache license](https://github.com/wgzhao/DataX/blob/toad/license.txt).









