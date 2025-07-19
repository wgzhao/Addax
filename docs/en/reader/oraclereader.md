# Oracle Reader

Oracle Reader plugin is used to read data from Oracle.

## Configuration Example

Configure a job to synchronize and extract data from Oracle database to local:

=== "job/oracle2stream.json"

  ```json
  --8<-- "jobs/oraclereader.json"
  ```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all configuration items of RDBMS Reader.

## Support for GEOMETRY Type

Starting from Addax `4.0.13`, experimental support for Oracle GEOMETRY type is provided. This plugin converts this type of data to JSON array strings.

Suppose you have such a table and data:

```sql
--8<-- "assets/sql/oracle_geom.sql"
```

The final output result of reading this table data is similar to the following:

```
--8<-- "assets/output/oracle_geom_reader.txt"
```

Note: This data type is currently in experimental support stage. The author's understanding of this data type is not deep, and it has not been comprehensively tested. Please do not use it directly in production environments.