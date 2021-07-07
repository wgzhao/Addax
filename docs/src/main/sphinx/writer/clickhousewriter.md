# ClickHouseWriter

ClickHouseWriter 插件实现了写入数据ClickHouse。在底层实现上，ClickHouseWriter 通过 JDBC 连接远程 ClickHouse 数据库，并执行相应的 `insert into ....` 语句将数据插入到ClickHouse库中。

## 示例

以下示例我们演示从 clickhouse 中读取一张表的内容，并写入到相同表结构的另外一张表中，用来测试插件所支持的数据结构

### 表结构以数据

假定要读取的表结构及数据如下：
```sql
CREATE TABLE ck_addax (
    c_int8 Int8,
    c_int16 Int16,
    c_int32 Int32,
    c_int64 Int64,
    c_uint8 UInt8,
    c_uint16 UInt16,
    c_uint32 UInt32,
    c_uint64 UInt64,
    c_float32 Float32,
    c_float64 Float64,
    c_decimal Decimal(38,10),
    c_string String,
    c_fixstr FixedString(36),
    c_uuid UUID,
    c_date Date,
    c_datetime DateTime('Asia/Chongqing'),
    c_datetime64 DateTime64(3, 'Asia/Chongqing'),
    c_enum Enum('hello' = 1, 'world'=2)
) ENGINE = MergeTree() ORDER BY (c_int8, c_int16) SETTINGS index_granularity = 8192;

insert into ck_addax values(
    127,
    -32768,
    2147483647,
    -9223372036854775808,
    255,
    65535,
    4294967295,
    18446744073709551615,
    0.999999999999,
    0.99999999999999999,
    1234567891234567891234567891.1234567891,
    'Hello String',
    '2c:16:db:a3:3a:4f',
    '5F042A36-5B0C-4F71-ADFD-4DF4FCA1B863',
    '2021-01-01',
    '2021-01-01 00:00:00',
    '2021-01-01 00:00:00',
    'hello'
);
```
要写入的表采取和读取表结构相同，其建表语句如下：

```sql
create table ck_addax_writer as ck_addax;
```

## 配置

以下为配置文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      }
    },
    "content": [
      {
        "writer": {
          "name": "clickhousewriter",
          "parameter": {
            "username": "default",
            "column": [
              "*"
            ],
            "connection": [
              {
                "table": [
                  "ck_addax_writer"
                ],
                "jdbcUrl": "jdbc:clickhouse://127.0.0.1:8123/default"
              }
            ],
            "preSql": ["alter table @table delete where 1=1"]
          }
        },
        "reader": {
          "name": "clickhousereader",
          "parameter": {
            "username": "default",
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:clickhouse://127.0.0.1:8123/"
                ],
                "table":["ck_addax"]
              }
            ]
          }
        }
      }
    ]
  }
}
```

将上述配置文件保存为   `job/clickhouse2clickhouse.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.py job/clickhouse2clickhouse.json
```

## 参数说明

| 配置项    | 是否必须 | 默认值 | 描述                                                                                                                                    |
| :-------- | :------: | ------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| jdbcUrl   |    是    | 无     | ClickHouse JDBC 连接信息 ,可按照官方规范填写连接附件控制信息。具体请参看[ClickHouse官方文档](https://github.com/yandex/clickhouse-jdbc) |
| username  |    是    | 无     | 数据源的用户名                                                                                                                          |
| password  |    否    | 无     | 数据源指定用户名的密码                                                                                                                  |
| table     |    是    | 无     | 所选取的需要同步的表 ,当配置为多张表时，用户自己需保证多张表是同一schema结构                                                            |
| column    |    是    | 无     | 所配置的表中需要同步的列名集合, 使用JSON的数组描述字段信息。用户使用 `*` 代表默认使用所有列配置，例如 `"['*']"`                         |
| batchSize |    否    | 2048   | 每次批量数据的条数                                                                                                                      |
