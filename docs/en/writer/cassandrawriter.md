# Cassandra Writer

Cassandra Writer plugin is used to write data to [Cassandra](https://cassandra.apache.org).

## Configuration Example

Configure a job to import data from memory to Cassandra:

=== "jobs/stream2cassandra.json"

```json
--8<-- "jobs/cassandrawriter.json"
```

## Parameters

| Configuration           | Required | Data Type | Default Value  | Description                                                  |
| :---------------------- | :------: | --------- | -------------- | ------------------------------------------------------------ |
| host                    | Yes      | string    | None           | Domain or IP of connection points, multiple nodes separated by commas |
| port                    | Yes      | int       | 9042           | Cassandra port                                               |
| username                | No       | string    | None           | Username of data source                                      |
| password                | No       | string    | None           | Password for specified username of data source              |
| useSSL                  | No       | boolean   | false          | Whether to use SSL connection                                |
| connectionsPerHost      | No       | int       | 8              | Client connection pool configuration: how many connections to establish with each server node |
| maxPendingPerConnection | No       | int       | 128            | Client connection pool configuration: maximum requests per connection |
| keyspace                | Yes      | string    | None           | Keyspace where the table to be synchronized is located      |
| table                   | Yes      | string    | None           | Selected table to be synchronized                            |
| column                  | Yes      | list      | None           | Collection of columns to be synchronized in the configured table |
| consistancyLevel        | No       | string    | `LOCAL_QUORUM` | Data consistency level                                       |
| batchSize               | No       | int       | 1              | Number of records in one batch submission (UNLOGGED BATCH)  |

### column

Content can be column names or `writetime()`. If a column name is configured as `writetime()`, the content of this column will be used as timestamp.

### consistancyLevel

Options: `ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY, TWO, THREE, LOCAL_ONE`

## Type Conversion

| Addax Internal Type | Cassandra Data Type                                                    |
| ------------------- | ---------------------------------------------------------------------- |
| Long                | int, tinyint, smallint,varint,bigint,time                              |
| Double              | float, double, decimal                                                 |
| String              | ascii,varchar, text,uuid,timeuuid,duration,list,map,set,tuple,udt,inet |
| Date                | date, timestamp                                                        |
| Boolean             | bool                                                                   |
| Bytes               | blob                                                                   |

Please note:

Currently does not support `counter` type and `custom` type.

## Constraints

### batchSize

1. Cannot exceed 65535
2. The content size in batch is limited by server-side `batch_size_fail_threshold_in_kb`.
3. If batch content exceeds `batch_size_warn_threshold_in_kb` limit, warn logs will be printed, but it doesn't affect writing and can be ignored.
4. If batch submission fails, all content in this batch will be rewritten record by record.