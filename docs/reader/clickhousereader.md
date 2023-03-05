# ClickHouse Reader

`ClickHouseReader` 插件支持从 [ClickHouse](https://clickhouse.tech)数据库读取数据。

## 示例

### 表结构及数据信息

假定需要的读取的表的结构以及数据如下：

```sql
--8<-- "sql/clickhouse.sql"
```

## 配置 json 文件

下面的配置文件表示从 ClickHouse 数据库读取指定的表数据并打印到终端

=== "job/clickhouse2stream.json"

```json
--8<-- "jobs/clickhousereader.json"
```

将上述配置文件保存为 `job/clickhouse2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/clickhouse2stream.json
```

其输出信息如下（删除了非关键信息)

```
--8<-- "output/clickhousereader.txt"
```

## 参数说明

`parameter` 配置项支持以下配置

| 配置项   | 是否必须 | 类型   | 默认值 | 描述                                                                                                                     |
| :------- | :------: | ------ | ------ | -------------------------------------------------------------------------------------------------------------------- |
| jdbcUrl  |    是    | array  | 无     | ClickHouse JDBC 连接信息 ,可按照官方规范填写连接附件控制信息。具体请参看[官方文档][1]                                          |
| username |    是    | string | 无     | 数据源的用户名                                                                                                                 |
| password |    否    | string | 无     | 数据源指定用户名的密码                                                                                                         |
| table    |    是    | array  | 无     | 所选取的需要同步的表 ,当配置为多张表时，用户自己需保证多张表是同一 schema 结构                                                 |
| column   |    是    | array  | 无     | 所配置的表中需要同步的列名集合, 使用 JSON 的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `["*"]`                |
| splitPk  |    否    | string | 无     | 希望使用 splitPk 代表的字段进行数据分片,Addax 因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能                   |
| autoPk   |    否    | bool   | false  | 是否自动猜测分片主键，`3.2.6` 版本引入                                                                                         |
| where    |    否    | string | 无     | 筛选条件                                                                                                                       |
| querySql |    否    | array  | 无     | 使用 SQL 查询而不是直接指定表的方式读取数据，当用户配置 querySql 时，ClickHouseReader 直接忽略 table、column、where 条件的配置 |

[1]: https://github.com/yandex/clickhouse-jdbc

## 支持的数据类型

目前 ClickHouseReader 支持大部分 ClickHouse 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 ClickHouseReader 针对 ClickHouse 类型转换列表:

| Addax 内部类型 | ClickHouse 数据类型                                                     |
| -------------- | ----------------------------------------------------------------------- |
| Long           | Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Enum8, Enum16 |
| Double         | Float32, Float64, Decimal                                               |
| String         | String, FixedString(N)                                                  |
| Date           | Date, DateTime, DateTime64                                              |
| Boolean        | UInt8                                                                   |
| Bytes          | String                                                                  |

## 限制

除上述罗列字段类型外，其他类型均不支持，如 Array、Nested 等
