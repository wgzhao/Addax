# TDengine Reader

TDengineReader 插件用于从涛思公司的 [TDengine](https://www.taosdata.com/cn/) 读取数据。

## 前置条件

考虑到性能问题，该插件使用了 TDengine 的 JDBC-JNI 驱动， 该驱动直接调用客户端 API（`libtaos.so` 或 `taos.dll`）将写入和查询请求发送到 taosd 实例。因此在使用之前需要配置好动态库链接文件。

首先将 `plugin/reader/tdenginereader/libs/libtaos.so.2.0.16.0` 拷贝到 `/usr/lib64` 目录，然后执行下面的命令创建软链接

```shell
ln -sf /usr/lib64/libtaos.so.2.0.16.0 /usr/lib64/libtaos.so.1
ln -sf /usr/lib64/libtaos.so.1 /usr/lib64/libtaos.so
```

## 示例

TDengine 数据自带了一个演示数据库 [taosdemo](https://www.taosdata.com/cn/getting-started/) , 我们从演示数据库读取部分数据并打印到终端

以下是配置文件

=== "job/tdengine2stream.json"

  ```json
  --8<-- "jobs/tdenginereader.json"
  ```

将上述配置文件保存为   `job/tdengine2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/tdengine2stream.json
```

命令输出类似如下：

```
--8<-- "output/tdenginereader.txt"
```

## 参数说明

| 配置项          | 是否必须 | 类型       | 默认值 |         描述   |
| :-------------- | :------: | ------ |------------- |--------------|
| jdbcUrl         |    是    | list | 无     | 对端数据库的JDBC连接信息，注意这里的 `TAOS` 必须大写 |
| username        |    是    | string | 无     | 数据源的用户名 |
| password        |    否    | string | 无     | 数据源指定用户名的密码 |
| table           |    是    | list | 无     | 所选取的需要同步的表名,使用JSON数据格式，当配置为多张表时，用户自己需保证多张表是同一表结构 |
| column          |    是    | list | 无     |  所配置的表中需要同步的列名集合，详细描述[rdbmreader](../rdbmsreader) |
| where           |    否    | string | 无     | 针对表的筛选条件 |
| querySql        |    否    | list | 无     | 使用自定义的SQL而不是指定表来获取数据，当配置了这一项之后，Addax系统就会忽略 `table`，`column`这些配置项 |

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
      "querySql": [
        "select * from test.meters where ts <'2017-07-14 10:40:02' and  loc='beijing' limit 100"
      ],
      "jdbcUrl": [
        "jdbc:TAOS-RS://127.0.0.1:6041/test"
      ],
      "driver": "com.taosdata.jdbc.rs.RestfulDriver"
    }
  ]
}
```

## 类型转换

| Addax 内部类型| TDengine 数据类型    |
| -------- | -----  |
| Long     | SMALLINT, TINYINT, INT, BIGINT, TIMESTAMP |
| Double   | FLOAT, DOUBLE|
| String   |  BINARY, NCHAR |
| Boolean  | BOOL   |

## 当前支持版本

TDengine 2.0.16

## 注意事项

- TDengine JDBC-JNI 驱动和动态库版本要求一一匹配，因此如果你的数据版本并不是 `2.0.16`，则需要同时替换动态库和插件目录中的JDBC驱动
