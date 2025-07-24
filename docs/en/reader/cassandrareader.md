# Cassandra Reader

`CassandraReader` plugin implements the ability to read data from [Cassandra](https://cassandra.apache.org).

## Configuration

Below is an example configuration for reading data from Cassandra to terminal

=== "job/cassandra2stream.json"

    ```json
    --8<-- "jobs/cassandrareader.json"
    ```

## Parameters

| Configuration | Required | Data Type | Default Value | Description |
| :------------ | :------: | --------- | ------------- | ----------- |
| host          | Yes      | list      | None          | Connection domain or IP, multiple nodes separated by commas |
| port          | Yes      | int       | 9042          | Port |
| username      | No       | string    | None          | Username |
| password      | No       | string    | None          | Password |
| useSSL        | No       | boolean   | false         | Whether to use SSL connection |
| keyspace      | Yes      | string    | None          | Keyspace where the table to be synchronized is located |
| table         | Yes      | string    | None          | Selected table to be synchronized |
| column        | Yes      | list      | None          | Collection of columns to be synchronized in the configured table, elements can specify column name or `writetime(column_name)`, the latter form reads timestamp of `column_name` column instead of data |
| where         | No       | string    | None          | Data filtering condition `cql` expression |
| allowFiltering| No       | boolean   | None          | Whether to filter data on server side, refer to official documentation for detailed description |
| consistencyLevel | No    | string    | LOCAL_QUORUM  | Data consistency level, options: `ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY, TWO, THREE, LOCAL_ONE` |