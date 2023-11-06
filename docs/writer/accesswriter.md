# Access Writer

AccessWriter 插件实现了写入数据到 [Access][1] 目的表的功能。

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

AccessWriter 基于 [rdbmswriter](../rdbmswriter) 实现，因此可以参考 rdbmswriter 的所有配置项。