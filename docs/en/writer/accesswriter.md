# Access Writer

Access Writer plugin implements the functionality of writing data to [Access](https://en.wikipedia.org/wiki/Microsoft_Access) destination tables.

## Example

Assume the Access table to be written has the following DDL statement:

```sql
create table tbl_test(name varchar(20), file_size int, file_date date, file_open boolean, memo blob);
```

Here we use data generated from memory to import into Access.

=== "job/stream2access.json"

    ```json
    --8<-- "jobs/accesswriter.json"
    ```

Save the above configuration file as `job/stream2access.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/stream2access.json
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer.

## Change Log

1. From version `5.0.1`, when the Access database file to be written does not exist, it will be automatically created and the database format will be set to `Access 2016`