# Sybase Reader

SybaseReader plugin implements reading data from [Sybase][1].

## Example

We can use Docker container to start a Sybase database

```shell
docker run -tid --rm  -h dksybase --name sybase  -p 5000:5000  ifnazar/sybase_15_7 bash /sybase/start
```

The following configuration reads this table to terminal:

=== "job/sybasereader.json"

  ```json
  --8<-- "jobs/sybasereader.json"
  ```

Save the above configuration file as `job/sybase2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/sybase2stream.json
```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all configuration items of RDBMS Reader.

[1]: https://en.wikipedia.org/wiki/Sybase