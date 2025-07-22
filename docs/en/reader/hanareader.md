# HANA Reader

HANA Reader plugin implements the ability to read data from SAP HANA.

## Example

The following configuration reads this table to terminal:

=== "job/hanareader.json"

  ```json
  --8<-- "jobs/hanareader.json"
  ```

Save the above configuration file as `job/hana2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/hana2stream.json
```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all parameters of RDBMS Reader.