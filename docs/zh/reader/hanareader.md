# HANA Reader

HANA Reader 插件实现了从 SAP HANA 读取数据的能力

## 示例


下面的配置是读取该表到终端的作业:

=== "job/hanareader.json"

  ```json
  --8<-- "jobs/hanareader.json"
  ```

将上述配置文件保存为   `job/hana2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/hana2stream.json
```

## 参数说明

该插件基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有参数。
