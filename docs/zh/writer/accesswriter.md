# Access Writer

Access Writer 插件实现了写入数据到 [Access](https://en.wikipedia.org/wiki/Microsoft_Access) 目的表的功能。

## 示例

假定要写入的 Access 表建表语句如下：

```sql
create table tbl_test(name varchar(20), file_size int, file_date date, file_open boolean, memo blob);
```

这里使用一份从内存产生到 Access 导入的数据。

=== "job/stream2access.json"

    ```json
    --8<-- "jobs/accesswriter.json"
    ```

将上述配置文件保存为 `job/stream2access.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/stream2access.json
```

## 参数说明

该插件基于 [RDBMS Writer](../rdbmswriter) 实现，因此可以参考 RDBMS Writer 的所有配置项。

## 变更记录

1. 从 `5.0.1` 版本其，当要写入的 Access 数据库文件不存在时，会自动创建，并设置数据库格式为 `Access 2016`