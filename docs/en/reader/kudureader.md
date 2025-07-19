# Kudu Reader

Kudu Reader plugin uses Kudu's Java client KuduClient to perform Kudu read operations.

## Configuration Example

We connect to kudu service through [Trino](https://trino.io)'s `kudu connector`, then perform table creation and data insertion.

### Table Creation and Data Insertion Statements

```sql
--8<-- "sql/kudu.sql"
```

### Configuration

The following is the configuration for reading kudu table and outputting to terminal:

=== "job/kudu2stream.json"

  ```json
  --8<-- "jobs/kudureader.json"
  ```

Save the above configuration file as `job/kudu2stream.json`

### Execution

Execute the following command for collection

```shell
bin/addax.sh job/kudu2stream.json
```

## Parameters

| Configuration          | Required | Type    | Default Value | Description                                            |
| :--------------------- | :------: | ------- | ------------- | ------------------------------------------------------ |
| masterAddress          | Yes      | string  | None          | Kudu Master cluster RPC address, multiple addresses separated by comma (,) |
| table                  | Yes      | string  | None          | Kudu table name                                        |
| splitPk                | No       | string  | None          | Parallel reading data shard field                     |
| lowerBound             | No       | string  | None          | Lower bound of parallel reading data shard range      |
| upperBound             | No       | string  | None          | Upper bound of parallel reading data shard range      |
| readTimeout            | No       | int     | 10            | Read data timeout (seconds)                            |
| scanTimeout            | No       | int     | 20            | Data scan request timeout (seconds)                    |
| column                 | No       | list    | None          | Specify fields to get                                  |
| where                  | No       | list    | None          | Specify other filter conditions, see description below |
| haveKerberos           | No       | boolean | false         | Whether to enable Kerberos authentication, if enabled, need to configure the following two items |
| kerberosKeytabFilePath | No       | string  | None          | Credential file path for Kerberos authentication, e.g. `/your/path/addax.service.keytab` |
| kerberosPrincipal      | No       | string  | None          | Credential principal for Kerberos authentication, e.g. `addax/node1@WGZHAO.COM` |

### where

`where` is used to define more filter conditions. It is an array type, where each element of the array is a filter condition, for example:

```json
{
  "where": ["age > 1", "user_name = 'wgzhao'"] 
}
```

The above defines two filter conditions. Each filter condition consists of three parts in the format `column operator value`:

- `column`: Field to filter
- `operator`: Comparison symbol, currently only supports `=`, `>`, `>=`, `<`, `<=`, `!=`. Other operators are not currently supported
- `value`: Comparison value

There are other limitations here that need special attention when using:

1. Multiple filter conditions have logical AND relationship between them, logical OR relationship is not supported yet

## Type Conversion

| Addax Internal Type | Kudu Data Type         |
| ------------------- | ---------------------- |
| Long                | byte, short, int, long |
| Double              | float, double, decimal |
| String              | string                 |
| Date                | timestamp              |
| Boolean             | boolean                |
| Bytes               | binary                 |