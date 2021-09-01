# 和阿里 DataX 的差异

`Addax` fork [DataX](https://github.com/alibaba/datax) 后，除核心框架未作太多改动后，其他均做了大幅改动，并一直在进行优化迭代。其差别说明如下：

## 精简

删除了仅限于阿里内部的数据库，这些数据库在非阿里集团无法使用，因此直接删除，包括：

- ADS
- DRDS
- OCS
- ODPS
- OSS
- OTS

## 增加

增加了部分插件，目前包括

### reader plugin

1. clickhousereader
2. datareader
3. dbffilereader
4. hbase20xreader
5. jsonfilereader
6. kudureader
7. influxdbreader
8. httpreader
9. elastichsearchreader
10. redisreader
11. sqlitereader
12. tdenginereader

### writer plugin

1. dbffilewrite
2. doriswriter
3. greenplumwriter
4. kuduwriter
5. influxdbwriter
6. rediswriter
7. sqlitewriter
8. tdenginewriter

### 部分插件增强功能，罗列如下

- 关系型数据库 增加了几乎所有基本数据类型和一部分复杂类型的支持， 支持表主键自动探测，大幅提升读取和写入性能
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
- rdbmsreader/rdbmswriter 增加了TDH Inceptor， Trino, PrestoSQL 查询引擎支持
- 尽可能减少了本地jar包的依赖，转为从maven仓库获取
- 绝大部分依赖包升级到了最新稳定版本，减少了潜在漏洞
- 不同插件下的相同依赖包做了版本统一
