# TDengine Writer

TDengine Writer plugin implements writing data to [TDengine](https://www.taosdata.com/cn/) database system. In the underlying implementation, TDengine Writer connects to remote TDengine database through JDBC JNI driver and executes corresponding SQL statements to batch write data to TDengine database.

## Prerequisites

Considering performance issues, this plugin uses TDengine's JDBC-JNI driver, which directly calls the client API (`libtaos.so` or `taos.dll`) to send write and query requests to `taosd` instances. Therefore, dynamic library link files need to be configured before use.

First copy `plugin/writer/tdenginewriter/libs/libtaos.so.2.0.16.0` to `/usr/lib64` directory, then execute the following commands to create soft links:

```shell
ln -sf /usr/lib64/libtaos.so.2.0.16.0 /usr/lib64/libtaos.so.1
ln -sf /usr/lib64/libtaos.so.1 /usr/lib64/libtaos.so
```

## Example

Assume the table to be written is as follows:

```sql
create table test.addax_test (
    ts timestamp,
    name nchar(100),
    file_size int,
    file_date timestamp,
    flag_open bool,
    memo nchar(100)
);
```

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer.