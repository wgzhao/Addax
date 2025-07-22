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

该插件基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有参数。

## 支持的数据类型

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
