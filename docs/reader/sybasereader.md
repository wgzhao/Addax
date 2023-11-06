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

SybaseReader 基于 [rdbmsreader](../rdbmsreader) 实现，因此可以参考 rdbmsreader 的所有配置项。

