# SQLite Reader

SQLiteReader 插件用于读取指定目录下的 sqlite 文件， 他继承于 [rdbmsreader](../rdbmsreader)

## 示例

我们创建示例文件：

```shell
$ sqlite3  /tmp/test.sqlite3
SQLite version 3.7.17 2013-05-20 00:56:22
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> create table test(id int, name varchar(10), salary double);
sqlite> insert into test values(1,'foo', 12.13),(2,'bar',202.22);
sqlite> .q
```

下面的配置是读取该表到终端的作业:

=== "job/sqlite2stream.json"

  ```json
  --8<-- "jobs/sqlitereader.json"
  ```

将上述配置文件保存为   `job/sqlite2stream.json`

### 执行采集命令

执行以下命令进行数据采集

```shell
bin/addax.sh job/sqlite2stream.json
```

## 参数说明

SQLiteReader 基于 [rdbmsreader](../rdbmsreader) 实现，因此可以参考 rdbmsreader 的所有配置项。

