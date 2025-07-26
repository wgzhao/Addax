# TDengine Reader

TDengine Reader plugin is used to read data from TDengine by TaosData [TDengine](https://www.taosdata.com/cn/).

## Prerequisites

Considering performance issues, this plugin uses TDengine's JDBC-JNI driver, which directly calls the client API (`libtaos.so` or `taos.dll`) to send write and query requests to taosd instances. Therefore, dynamic library link files need to be configured before use.

First copy `plugin/reader/tdenginereader/libs/libtaos.so.2.0.16.0` to `/usr/lib64` directory, then execute the following commands to create soft links:

```shell
ln -sf /usr/lib64/libtaos.so.2.0.16.0 /usr/lib64/libtaos.so.1
ln -sf /usr/lib64/libtaos.so.1 /usr/lib64/libtaos.so
```

## Example

TDengine comes with a demo database [taosdemo](https://www.taosdata.com/cn/getting-started/). We read some data from the demo database and print to terminal.

The following is the configuration file:

=== "job/tdengine2stream.json"

  ```json
  --8<-- "jobs/tdenginereader.json"
  ```

Save the above configuration file as `job/tdengine2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/tdengine2stream.json
```

Command output is similar to the following:

```
--8<-- "output/tdenginereader.txt"
```

## Parameters

| Configuration   | Required | Type   | Default Value | Description                                                              |
| :-------------- | :------: | ------ | ------------- | ------------------------------------------------------------------------ |
| jdbcUrl         | Yes      | list   | None          | JDBC connection information of target database, note that `TAOS` here must be uppercase |
| username        | Yes      | string | None          | Username of data source                                                  |
| password        | No       | string | None          | Password for specified username of data source                           |
| table           | Yes      | list   | None          | Selected table names to be synchronized, using JSON data format. When configured for multiple tables, users need to ensure multiple tables have the same structure |
| column          | Yes      | list   | None          | Collection of column names to be synchronized in configured table, detailed description [rdbmreader](../rdbmsreader) |
| where           | No       | string | None          | Filtering conditions for the table                                       |
| querySql        | No       | list   | None          | Use custom SQL instead of specified table to get data. When this item is configured, Addax system will ignore `table`, `column` configuration items |
| beginDateTime   | Yes      | string | None          | Data start time, Job migrates data from `beginDateTime` to `endDateTime`, format is `yyyy-MM-dd HH:mm:ss` |
| endDateTime     | Yes      | string | None          | Data end time, Job migrates data from `beginDateTime` to `endDateTime`, format is `yyyy-MM-dd HH:mm:ss` |
| splitInterval   | Yes      | string | None          | Divide `task` according to `splitInterval`, create one `task` per `splitInterval` |

### splitInterval

Used to divide `task`. For example, `20d` represents dividing data into 1 `task` every 20 days. Configurable time units:

- `d` (day)
- `h` (hour)
- `m` (minute)
- `s` (second)

### Using JDBC-RESTful Interface

If you don't want to depend on local libraries or don't have permissions, you can use the `JDBC-RESTful` interface to write to tables. Compared to JDBC-JNI, the configuration differences are:

- driverClass specified as `com.taosdata.jdbc.rs.RestfulDriver`
- jdbcUrl starts with `jdbc:TAOS-RS://`
- Use `6041` as connection port

So the `connection` in the above configuration should be modified as follows:

```json
{
  "connection": [
    {
      "querySql": [
        "select * from test.meters where ts <'2017-07-14 10:40:02' and  loc='beijing' limit 100"
      ],
      "jdbcUrl": [
        "jdbc:TAOS-RS://127.0.0.1:6041/test"
      ],
      "driver": "com.taosdata.jdbc.rs.RestfulDriver"
    }
  ]
}
```

## Type Conversion

| Addax Internal Type | TDengine Data Type                        |
| ------------------- | ----------------------------------------- |
| Long                | SMALLINT, TINYINT, INT, BIGINT, TIMESTAMP |
| Double              | FLOAT, DOUBLE                             |
| String              | BINARY, NCHAR                             |
| Boolean             | BOOL                                      |

## Currently Supported Versions

TDengine 2.0.16

## Notes

- TDengine JDBC-JNI driver and dynamic library versions must match one-to-one. Therefore, if your data version is not `2.0.16`, you need to replace both the dynamic library and JDBC driver in the plugin directory.