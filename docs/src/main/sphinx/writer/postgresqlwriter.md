# Postgresql Writer

PostgresqlWriter插件实现了写入数据到 [PostgreSQL](https://postgresql.org) 数据库库表的功能。

## 示例

以下配置演示从postgresql指定的表读取数据，并插入到具有相同表结构的另外一张表中，用来测试该插件所支持的数据类型。

### 表结构信息

假定建表语句以及输入插入语句如下：

```sql
create table if not exists addax_tbl 
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
insert into addax_tbl values(
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
create table addax_tbl1 like addax_tbl;
```

### 任务配置

以下是配置文件

```json
{
  "job": {
    "setting": {
      "speed": {
        "byte": -1,
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "pgtest",
            "password": "pgtest",
            "column": [
              "*"
            ],
            "connection": [
              {
                "table": [
                  "addax_tbl"
                ],
                "jdbcUrl": [
                  "jdbc:postgresql://localhost:5432/pgtest"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "column": [
              "*"
            ],
            "preSql": [
              "truncate table @table"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:postgresql://127.0.0.1:5432/pgtest",
                "table": [
                  "addax_tbl1"
                ]
              }
            ],
            "username": "pgtest",
            "password": "pgtest",
            "writeMode": "insert"
          }
        }
      }
    ]
  }
}
```

将上述配置文件保存为  `job/pg2pg.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/pg2pg.json
```

## 参数说明

| 配置项          | 是否必须 | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |
| jdbcUrl         |    是    | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范，并可以填写连接 [附件控制信息](http://jdbc.postgresql.org/documentation/93/connect.html)  |
| username        |    是    | 无     | 数据源的用户名 |
| password        |    否    | 无     | 数据源指定用户名的密码 |
| writeMode       |    否    | insert     | 写入模式，支持insert, update 详见如下 |
| table           |    是    | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | 无     |  所配置的表中需要同步的列名集合，详细描述见 [rdbmswriter](rdbmswriter) |
| preSql         |    否    | 无     | 执行数据同步任务之前率先执行的sql语句，目前只允许执行一条SQL语句，例如清除旧数据,涉及到的表可用 `@table`表示 |
| postSql        |   否      | 无    | 执行数据同步任务之后执行的sql语句，目前只允许执行一条SQL语句，例如加上某一个时间戳|
| batchSize       |    否    | 1024   | 定义了插件和数据库服务器端每次批量数据获取条数，调高该值可能导致 Addax 出现OOM或者目标数据库事务提交失败导致挂起 |

### writeMode

默认情况下， 采取 `insert into ` 语法写入 postgresql 表，如果你希望采取主键存在时更新，不存在则写入的方式， 可以使用 `update` 模式。假定表的主键为 `id` ,则 `writeMode` 配置方法如下：

```
"writeMode": "update(id)"
```

如果是联合唯一索引，则配置方法如下：

```
"writeMode": "update(col1, col2)"
```

注： `update` 模式在 `3.1.6` 版本首次增加，之前版本并不支持。

## 类型转换

目前 PostgresqlWriter支持大部分 PostgreSQL类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 PostgresqlWriter针对 PostgreSQL类型转换列表:

| Addax 内部类型| PostgreSQL 数据类型    |
| -------- | -----  |
| Long     |bigint, bigserial, integer, smallint, serial |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit, inet,cidr,macaddr,uuid,xml,json|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|

## 已知限制

除以上列出的数据类型外，其他数据类型理论上均为转为字符串类型，但不确保准确性