# SQLServer Reader

SqlServerReader plugin is used to read data from SQLServer.

## Configuration Example

Configure a job to synchronize and extract data from SQLServer database to local:

=== "job/sqlserver2stream.json"

  ```json
  --8<-- "jobs/sqlserverreader.json"
  ```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all configuration items of RDBMS Reader.