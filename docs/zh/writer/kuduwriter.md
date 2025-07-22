# Kudu Writer 

Kudu Writer 插件实现了将数据写入到 [kudu](https://kudu.apache.org) 的能力，当前是通过调用原生RPC接口来实现的。
后期希望通过 [impala](https://impala.apache.org) 接口实现，从而增加更多的功能。

## 示例

以下示例演示了如何从内存读取样例数据并写入到 kudu 表中的。

### 表结构

我们用 [trino](https://trino.io) 工具连接到 kudu 服务，然后通过下面的 SQL 语句创建表

```sql
CREATE TABLE kudu.default.users (
  user_id int WITH (primary_key = true),
  user_name varchar,
  salary double
) WITH (
  partition_by_hash_columns = ARRAY['user_id'],
  partition_by_hash_buckets = 2
);
```

### job 配置文件

创建 `job/stream2kudu.json` 文件，内容如下：

=== "job/stream2kudu.json"

  ```json
  --8<-- "jobs/kuduwriter.json"
  ```

### 运行

执行下下面的命令进行数据采集

```bash
bin/addax.sh job/stream2kudu.json
```

## 参数说明

| 配置项        | 是否必须 | 类型    | 默认值 | 描述                                                                   |
| :------------ | :------: | ------- | ------ | ---------------------------------------------------------------------- |
| masterAddress |    是    | string  | 无     | Kudu Master集群RPC地址,多个地址用逗号(,)分隔                           |
| table         |    是    | string  | 无     | kudu 表名                                                              |
| writeMode     |    否    | string  | upsert | 表数据写入模式，支持 upsert, insert 两者                               |
| timeout       |    否    | int     | 100    | 写入数据超时时间(秒), 0 表示不受限制                                   |
| column        |    是    | list    | 无     | 要写入的表字段，配置方式见上示例                                 |
| skipFail      |    否    | boolean | false  | 是否跳过插入失败的记录，如果设置为true，则插件不会把插入失败的当作异常 |
| haveKerberos           |  否   | boolean     | false   | 是否启用 Kerberos 认证，如果启用，则需要同时配置以下两项                              |
| kerberosKeytabFilePath |  否   | string      | 无       | 用于 Kerberos 认证的凭证文件路径, 比如 `/your/path/addax.service.keytab`    |
| kerberosPrincipal      |  否   | string      | 无       | 用于 Kerberos 认证的凭证主体, 比如 `addax/node1@WGZHAO.COM`               |

## column

`column` 可以直接指定要写入的列，如同上述例子，也可以设置 `["*"]` 来表示写入所有列。
