# Sybase Reader

SybaseReader 插件实现了从 [Sybase][1] 读取数据

## 示例

我们可以用 Docker 容器来启动一个 Sybase 数据库

```shell
docker run -tid --rm  -h dksybase --name sybase  -p 5000:5000  ifnazar/sybase_15_7 bash /sybase/start
```

下面的配置是读取该表到终端的作业:

=== "job/sybasereader.json"

  ```json
  --8<-- "jobs/sybasereader.json"
  ```

将上述配置文件保存为   `job/sybase2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/sybase2stream.json
```

## 参数说明

| 配置项       | 是否必须 | 类型     | 默认值   | 描述                                                               |
|:----------|:----:|--------|-------|------------------------------------------------------------------|
| jdbcUrl   |  是   | list   | 无     | 对端数据库的JDBC连接信息，jdbcUrl按照RDBMS官方规范.                               |
| driver    |  否   | string | 无     | 自定义驱动类名，解决兼容性问题，详见下面描述                                           |
| username  |  是   | string | 无     | 数据源的用户名                                                          |
| password  |  否   | string | 无     | 数据源指定用户名的密码                                                      |
| table     |  是   | list   | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构                 |
| column    |  是   | list   | 无     | 所配置的表中需要同步的列名集合，详细描述 [rdbmreader](../rdbmsreader)                |
| splitPk   |  否   | string | 无     | 使用splitPk代表的字段进行数据分片，详细描述见 [rdbmreader](../rdbmsreader)          |
| autoPk    |  否   | bool   | false | 是否自动猜测分片主键，`3.2.6` 版本引入                                          |
| where     |  否   | string | 无     | 针对表的筛选条件                                                         |
| querySql  |  否   | list   | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |
| fetchSize |  否   | int    | 2048  | Sybase 要求配置 fetchSize 来提升性能                                      |


## 类型转换

目前 SybaseReader 支持大部分 Sybase 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出类型转换列表:


| Addax 内部类型 | SqlServer 数据类型                                                    |
|------------|-------------------------------------------------------------------|
| Long       | bigint, int, smallint, tinyint                                    |
| Double     | float, decimal, real, numeric                                     |
| String     | char,nchar,ntext,nvarchar,text,varchar,nvarchar(MAX),varchar(MAX) |
| Date       | date, datetime, time                                              |
| Boolean    | bit                                                               |
| Bytes      | binary,varbinary,varbinary(MAX),timestamp, image                  |


[1]: https://www.sap.com/products/technology-platform/sql-anywhere.html