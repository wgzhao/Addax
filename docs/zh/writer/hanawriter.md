# HANA Writer

HANA Writer 插件实现了写入数据到 [SAP HANA](https://www.sap.com/products/hana.html) 目的表的功能。

## 示例

假定要写入的 HANA 表建表语句如下：

```sql
create table system.addax_tbl
(
col1 varchar(200) ,
col2 int(4),
col3 date,
col4 boolean,
col5 clob
);
```

这里使用一份从内存产生到 HANA 导入的数据。

=== "job/hanawriter.json"

```json
--8<-- "jobs/hanawriter.json"
```

将上述配置文件保存为 `job/hana2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/hana2stream.json
```

## 参数说明

该插件基于 [RDBMS Writer](../rdbmswriter) 实现，因此可以参考 RDBMS Writer 的所有配置项。


