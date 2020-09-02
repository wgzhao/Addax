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

## 文档

[在线文档](https://datax.readthedocs.io)
[项目内文档](docs/src/main/sphinx/index.rst)

## License

This software is free to use under the Apache License [Apache license](/license.txt).
