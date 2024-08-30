# TDengine Writer

TDengine Writer 插件实现了将数据写入 [TDengine](https://www.taosdata.com/cn/) 数据库系统。在底层实现上，TDengine Writer 通过JDBC JNI 驱动连接远程 TDengine 数据库，
并执行相应的sql语句将数据批量写入 TDengine 库中。

## 前置条件

考虑到性能问题，该插件使用了 TDengine 的 JDBC-JNI 驱动， 该驱动直接调用客户端 API（`libtaos.so` 或 `taos.dll`）将写入和查询请求发送到 `taosd` 实例。
因此在使用之前需要配置好动态库链接文件。

首先将 `plugin/writer/tdenginewriter/libs/libtaos.so.2.0.16.0` 拷贝到 `/usr/lib64` 目录，然后执行下面的命令创建软链接

```shell
ln -sf /usr/lib64/libtaos.so.2.0.16.0 /usr/lib64/libtaos.so.1
ln -sf /usr/lib64/libtaos.so.1 /usr/lib64/libtaos.so
```

## 示例

假定要写入的表如下：

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

以下是配置文件

=== "job/stream2tdengine.json"

  ```json
  --8<-- "jobs/tdenginewriter.json"
  ```

将上述配置文件保存为   `job/stream2tdengine.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/tdengine2stream.json
```

命令输出类似如下：

```
--8<-- "output/tdenginewriter.txt"
```

## 参数说明

该插件基于 [RDBMS Writer](../rdbmswriter) 实现，因此可以参考 RDBMS Writer 的所有配置项，并且增加了一些 TDengine 特有的配置项。

### 使用 JDBC-RESTful 接口

如果不想依赖本地库，或者没有权限，则可以使用 `JDBC-RESTful` 接口来写入表，相比 JDBC-JNI 而言，配置区别是：

- driverClass 指定为 `com.taosdata.jdbc.rs.RestfulDriver`
- jdbcUrl 以 `jdbc:TAOS-RS://` 开头；
- 使用 `6041` 作为连接端口

所以上述配置中的 `connection` 应该修改为如下：

```json
{
  "connection": [
    {
      "jdbcUrl": "jdbc:TAOS-RS://127.0.0.1:6041/test",
      "table": [
        "addax_test"
      ],
      "driver": "com.taosdata.jdbc.rs.RestfulDriver"
    }
  ]
}
```

## 类型转换

目前 TDenginereader 支持 TDengine 所有类型，具体如下

| Addax 内部类型 | TDengine 数据类型                         |
| -------------- | ----------------------------------------- |
| Long           | SMALLINT, TINYINT, INT, BIGINT, TIMESTAMP |
| Double         | FLOAT, DOUBLE                             |
| String         | BINARY, NCHAR                             |
| Boolean        | BOOL                                      |

## 当前支持版本

TDengine 2.0.16

## 注意事项

- TDengine JDBC-JNI 驱动和动态库版本要求一一匹配，因此如果你的数据版本并不是 `2.0.16`，则需要同时替换动态库和插件目录中的JDBC驱动
- TDengine 的时序字段（timestamp）默认最小值为 `1500000000000`，即 `2017-07-14 10:40:00.0`，如果你写入的时许时间戳小于该值，则会报错
