# 和阿里 DataX 的差异

> DataX 官方仓库自 2023 年 9 月（`datax_v202309`）后不再发布新版本，Addax 是其持续维护的继承者。

`Addax` fork [DataX](https://github.com/alibaba/datax) 后，保留了 Framework + Plugin 的插件化架构，但在运行时、插件、工程化三个方面做了大量改动并持续迭代。差异主要体现为：**核心框架增强、插件调整（精简 / 新增 / 增强）、工程与维护**，下面分别说明。

## 核心框架增强

- **作业配置同时支持 JSON 和 YAML** 两种格式，DataX 仅支持 JSON
- **密码加密**：通过 `encrypt_password.sh` 将作业配置中的明文密码替换为 `${enc:...}` 密文，避免敏感信息明文暴露
- **addax-server**：内置 HTTP 服务模块，通过 REST 接口提交任务、查询任务状态和结果，支持并发数限制，可用 HTTP 接口运维而不仅依赖命令行
- **Transformer 增强**：内置更多 UDF（`dx_substr`、`dx_pad`、`dx_replace`、脱敏、补全、过滤、类型转换等），并支持 **Groovy 自定义转换脚本**
- **统计上报**：任务执行结果（耗时、行数、字节数等）可通过 HTTP POST 上报到指定服务器，便于集中监控
- **运行时升级**：
  - 基准运行环境升级为 **JDK 17**，并兼容 JDK 21+（DataX 停留在 JDK 8）
  - 默认使用 **ZGC** 垃圾回收器，相比 DataX 的 G1 延迟更低
  - 使用 JDK 内置 **HttpClient** 替代 Apache HttpClient，减少外部依赖
- **性能优化**：rdbms 模块使用共享连接池缓存，避免每次查询重复创建/销毁数据源；通信与格式化等热点代码做了线程安全与性能优化
- **通配符列**：支持 `column: "*"` 配置，无需枚举全部列即可同步所有字段（MongoDB、流式插件等）
- **命令行增强**：`addax.sh` 支持 `-p` 注入作业变量、`-L` 设置日志级别、`-d` 调试模式、`-j` 自定义 JVM 参数、自定义日志文件名
- **测试支持**：内置 `datareader`（假数据生成器），便于快速验证链路和压测

## 精简

删除了以下 DataX 自带的插件：

- **阿里生态/国内特定服务**：ADS、DRDS、OCS、ODPS、OSS、OTS、ADB（MySQL/PG）、DataHub、Hologres、LogHub、OceanBase、GaussDB、GDB、KingbaseES、SelectDB、TSDB 等。这些插件依赖阿里内部环境或特定云服务，在通用场景下无法使用
- **老旧的 HBase 版本**：移除 hbase094x 系列，统一到 hbase11x / hbase20x / hbase11xsql / hbase20xsql

## 新增插件

以下插件为 DataX 所没有，由 Addax 新增：

### Reader

1. accessreader（Access 数据库）
2. databendreader（Databend）
3. datareader（假数据生成器）
4. dbfreader（DBF 文件）
5. elasticsearchreader（Elasticsearch，DataX 仅有 writer）
6. excelreader（Excel 文件）
7. hanareader（SAP HANA）
8. hbase20xreader（HBase 2.x，DataX 仅有老版本）
9. hivereader（Hive 表）
10. httpreader（HTTP 接口，支持鉴权与 JSON 请求体）
11. influxdbreader（InfluxDB）
12. influxdb2reader（InfluxDB 2.x）
13. jsonfilereader（JSON 文件）
14. kafkareader（Kafka）
15. kudureader（Kudu，DataX 仅有 writer）
16. redisreader（Redis）
17. s3reader（Amazon S3 及兼容对象存储）
18. sqlitereader（SQLite）

### Writer

1. accesswriter（Access 数据库）
2. dbfwriter（DBF 文件）
3. excelwriter（Excel 文件，直接生成 XML 的快速写入）
4. greenplumwriter（Greenplum）
5. hanawriter（SAP HANA）
6. icebergwriter（Apache Iceberg）
7. influxdbwriter（InfluxDB）
8. influxdb2writer（InfluxDB 2.x）
9. kafkawriter（Kafka）
10. paimonwriter（Apache Paimon）
11. rediswriter（Redis）
12. s3writer（S3，支持 ORC/Parquet 存储格式）
13. sqlitewriter（SQLite）

> 注：部分插件 DataX 后来也提供（如 clickhouse、tdengine、starrocks、databendwriter 等），但实现各自独立演进，Addax 的版本仍在持续维护和增强。

## 插件增强

- **关系型数据库（rdbms）**：
  - 支持几乎所有基本数据类型和一部分复杂类型，包括 MySQL GEOMETRY、UNSIGNED BIGINT 溢出处理、TIME 微秒精度等
  - 支持表主键自动探测，大幅提升读取和写入性能
  - 支持 `querySql` 从外部 SQL 文件读取查询语句
  - 列名引用（quoting）机制增强，支持 `excludeColumn` 排除列
  - 支持 TDH Inceptor、Trino、PrestoSQL 查询引擎
- **写入语法增强**：oraclewriter 支持 `merge into`，postgresqlwriter 支持 `insert ... on conflict`
- **Doris**：doriswriter 基于 Stream Load 异步批量写入，性能和可靠性大幅提升；dorisreader 支持 Arrow Flight SQL 协议
- **HDFS**：hdfswriter 支持 Decimal 类型、Parquet 格式、更多压缩格式、目录覆盖模式、自动创建路径、ORC Bloom Filter、TIMESTAMP 纳秒精度；hdfsreader 支持 Parquet、更多压缩格式、隐藏文件过滤
- **MongoDB**：reader/writer 均支持通配符列（`*`）、嵌套字段（点号路径）和对象数组列
- **HBase**：hbase11xsqlwriter 支持 Kerberos 认证
- **Kudu**：reader/writer 均支持 Kerberos 认证
- **Excel**：writer 直接生成 XML 文件，写入性能大幅提升
- **Stream**：streamwriter 二进制列输出 hex 预览，终端与日志输出保持一致
- **安装部署**：提供 `install.sh` 一键安装脚本（Linux/macOS）和 Docker 镜像，支持快速部署

## 工程与维护

- 移除本地 jar 包依赖，全部改为从 Maven 仓库获取；相同依赖在多个插件间统一版本管理
- 依赖持续升级到最新稳定版本，及时修复已知安全漏洞
- 支持 `build-module.sh` 单独构建某个插件模块，无需整包编译
- **月度发布版本**（维护版，见 [RELEASING.md](RELEASING.md)），issue 响应及时，社区 PR 持续被评审合并
