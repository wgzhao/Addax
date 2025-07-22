# Access Reader

AccessReader 实现了从 [Access](https://en.wikipedia.org/wiki/Microsoft_Access) 数据库上读取数据的能力，他基于 [Addax RDBMS Reader](../rdbmsreader) 实现。

## 示例

我们下载用于测试用的 [AcessThemeDemo.zip](http://www.databasedev.co.uk/downloads/AccessThemeDemo.zip) 文件，解压后得到 `AccessThemeDemo.mdb` 文件，该文件中包含了一个 `tbl_Users` 表，我们将该表的数据同步到终端上。

下面的配置是读取该表到终端的作业:

=== "job/access2stream.json"

```json
--8<-- "jobs/accessreader.json"
```

将上述配置文件保存为 `job/access2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/access2stream.json
```

## 参数说明

AccessReader 基于 [RDBMS Reader](../rdbmsreader) 实现，因此可以参考 RDBMS Reader 的所有配置项。
