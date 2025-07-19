# SQLite Reader

SQLite Reader plugin is used to read sqlite files in a specified directory. It inherits from [RDBMS Reader](../rdbmsreader).

## Example

We create an example file:

```shell
$ sqlite3  /tmp/test.sqlite3
SQLite version 3.7.17 2013-05-20 00:56:22
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> create table test(id int, name varchar(10), salary double);
sqlite> insert into test values(1,'foo', 12.13),(2,'bar',202.22);
sqlite> .q
```

The following configuration reads this table to terminal:

=== "job/sqlite2stream.json"

  ```json
  --8<-- "jobs/sqlitereader.json"
  ```

Save the above configuration file as `job/sqlite2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/sqlite2stream.json
```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all configuration items of RDBMS Reader.