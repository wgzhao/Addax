# Kudu Reader

Kudu Reader 插件利用 Kudu 的 java客户端 KuduClient 进行 Kudu 的读操作。

## 配置示例

我们通过 [Trino](https://trino.io)  的 `kudu connector` 连接 kudu 服务，然后进行表创建以及数据插入

### 建表语句以及数据插入语句

```sql
--8<-- "sql/kudu.sql"
```

### 配置

以下是读取kudu表并输出到终端的配置

=== "job/kudu2stream.json"

  ```json
  --8<-- "jobs/kudureader.json"
  ```

把上述配置文件保存为 `job/kudu2stream.json`

### 执行

执行下面的命令进行采集

```shell
bin/addax.sh job/kudu2stream.json
```

## 参数说明

| 配置项        | 是否必须 | 类型   | 默认值 | 描述                                         |
| :------------ | :------: | ------ | ------ | -------------------------------------------- |
| masterAddress |   是   | string | 无     | Kudu Master 集群RPC地址,多个地址用逗号(,)分隔 |
| table         |   是   | string | 无     | kudu 表名                                    |
| splitPk       |    否    | string | 无     | 并行读取数据分片字段                         |
| lowerBound    |    否    | string | 无     | 并行读取数据分片范围下界                     |
| upperBound    |    否    | string | 无     | 并行读取数据分片范围上界                     |
| readTimeout   |    否    | int    | 10     | 读取数据超时(秒)                             |
| scanTimeout   |    否    | int    | 20     | 数据扫描请求超时(秒)                         |
| column        |    否    | list   | 无     | 指定要获取的字段                             |
| where         |    否    | list   | 无     | 指定其他过滤条件，详见下面描述               |
| haveKerberos           |  否   | boolean     | false   | 是否启用 Kerberos 认证，如果启用，则需要同时配置以下两项                              |
| kerberosKeytabFilePath |  否   | string      | 无       | 用于 Kerberos 认证的凭证文件路径, 比如 `/your/path/addax.service.keytab`    |
| kerberosPrincipal      |  否   | string      | 无       | 用于 Kerberos 认证的凭证主体, 比如 `addax/node1@WGZHAO.COM`               |

### where

`where` 用来定制更多的过滤条件，他是一个数组类型，数组的每个元素都是一个过滤条件，比如

```json
{
  "where": ["age > 1", "user_name = 'wgzhao'"] 
}
```

上述定义了两个过滤条件，每个过滤条件由三部分组成，格式为  `column operator value`

- `column`: 要过滤的字段
- `operator`: 比较符号，当前仅支持 `=`,  `>`, `>=`, `<`, `<=` , `!=` 其他操作符号当前还不支持
- `value`: 比较值

这里还有其他一些限定，在使用时，要特别注意：

1. 多个过滤条件之间的逻辑与关系(`AND`)，暂不支持逻辑或(`OR`)关系

## 类型转换

| Addax 内部类型 | Kudu 数据类型          |
| -------------- | ---------------------- |
| Long           | byte, short, int, long |
| Double         | float, double, decimal |
| String         | string                 |
| Date           | timestamp              |
| Boolean        | boolean                |
| Bytes          | binary                 |
