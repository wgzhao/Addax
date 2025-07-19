# InfluxDB2 Reader

InfluxDB2 Reader plugin implements reading data from [InfluxDB](https://www.influxdata.com) version 2.0 and above.

Note: If your InfluxDB is version 1.8 and below, you should use the [InfluxDBReader](../influxdbreader/) plugin.

## Example

The following example demonstrates how this plugin reads data from specified tables (i.e., metrics) and outputs to terminal.

### Create Job File

Create `job/influx2stream.json` file with the following content:

=== "job/influx2stream.json"

  ```json
  --8<-- "jobs/influx2stream.json"
  ```

### Run

Execute the following command for data collection

```bash
bin/addax.sh job/influx2stream.json
```

## Parameters

| Configuration | Required | Data Type | Default Value | Description                                                             |
| :------------ | :------: | --------- | ------------- | ----------------------------------------------------------------------- |
| endpoint      | Yes      | string    | None          | InfluxDB connection string                                              |
| token         | Yes      | string    | None          | Token for accessing database                                            |
| table         | No       | list      | None          | Selected table names (i.e., metrics) to be synchronized               |
| org           | Yes      | string    | None          | Specify InfluxDB org name                                               |
| bucket        | Yes      | string    | None          | Specify InfluxDB bucket name                                            |
| column        | No       | list      | None          | Collection of column names to be synchronized in configured table, detailed description see [rdbmreader][1] |
| range         | Yes      | list      | None          | Time range for reading data                                             |
| limit         | No       | int       | None          | Limit number of records to get                                          |

### column

If `column` is not specified, or `column` is specified as `["*"]`, all valid `_field` fields and `_time` field will be read.
Otherwise, read according to specified fields.

### range

`range` is used to specify the time range for reading metrics, with the following format:

```json
{
  "range": ["start_time", "end_time"]
}
```

`range` consists of a list of two strings, the first string represents start time, the second represents end time. The time expression format must comply with [Flux format requirements][2], like this:

```json
{
  "range": ["-15h", "-2h"]
}
```

If you don't want to specify the second end time, you can omit it, like this:

```json
{
  "range": ["-15h"]
}
```

## Type Conversion

Current implementation treats all fields as strings.

## Limitations

1. Current plugin only supports version 2.0 and above

[1]: ../rdbmsreader
[2]: https://docs.influxdata.com/influxdb/v2.0/query-data/flux/