# GreenplumWriter

GreenplumWriter 插件使用 `copy from` 语法 将数据写入 [Greenplum](https://greenplum.org) 数据库。

## 示例

以下配置演示从postgresql指定的表读取数据，并插入到具有相同表结构的另外一张表中，用来测试该插件所支持的数据类型。

```sql
create table if not exists datax_tbl
(
c_bigint bigint,
c_bit bit(3),
c_bool boolean,
c_byte bytea,
c_char char(10),
c_varchar varchar(20),
c_date  date,
c_double float8,
c_int integer,
c_json json,
c_number decimal(8,3),
c_real  real,
c_small smallint,
c_text  text,
c_ts timestamp,
c_uuid uuid,
c_xml xml,
c_money money,
c_inet inet,
c_cidr cidr,
c_macaddr macaddr
);
insert into datax_tbl values(
999988887777,
B'101',
TRUE,
'\xDEADBEEF',
'hello',
'hello, world',
'2021-01-04',
999888.9972,
9876542,
'{"bar": "baz", "balance": 7.77, "active": false}'::json,
12345.123,
123.123,
126,
'this is a long text ',
'2020-01-04 12:13:14',
'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
'<foo>bar</foo>'::xml,
'52093.89'::money,
'192.168.1.1'::inet,
'192.168.1/24'::cidr,
'08002b:010203'::macaddr
);
```

创建需要插入的表的语句如下:

```sql
create table gp_test ( like datax_tbl);
```

### 任务配置

以下是配置文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "wgzhao",
            "password": "wgzhao",
            "column": [
              "*"
            ],
            "connection": [
              {
                "table": [
                  "datax_tbl"
                ],
                "jdbcUrl": [
                  "jdbc:postgresql://localhost:5432/wgzhao"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "greenplumwriter",
          "parameter": {
            "username": "wgzhao",
            "password": "wgzhao",
            "queueSize": 5,
            "numProc": 2,
            "numWriter": 1,
            "column": [
              "*"
            ],
            "preSql": [
              "truncate table @table"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:postgresql://localhost:5432/wgzhao",
                "table": [
                  "gp_test"
                ]
              }
            ]
          }
        }
      }
    ]
  }
}
```

将上述配置文件保存为  `job/pg2gp.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/datax.py job/pg2gp.json
```

## 参数说明

| 配置项          | 是否必须 | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接[附件控制信息](http://jdbc.postgresql.org/documentation/93/connect.html)  ｜
| username        |    是    | 无     | 数据源的用户名 |
| password        |    否    | 无     | 数据源指定用户名的密码 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述[rdbmswriter](rdbmswriter.md) |
| preSql         |    否    | 无     | 执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据,涉及到的表可用 `@table`表示 |
| postSql        |   否      | 无    | 执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳|
| queueSize      | 否       | 1000   | 线程队列大小，增大此参数增加内存消耗，提升性能 |
| numProc        | 否       | 4     | 用于进行格式化数据的线程数 |
| numWriter      | 否       | 1     | 写入数据库的并发数 | 

### 类型转换

目前 GreenplumWriter 支持大部分 Greenplum 数据类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 GreenplumWriter 针对 Greenplum 类型转换列表:

| DataX 内部类型| Greenplum 数据类型    |
| -------- | -----  |
| Long     |bigint, bigserial, integer, smallint, serial |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit, inet,cidr,macaddr,uuid,xml,json|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|


