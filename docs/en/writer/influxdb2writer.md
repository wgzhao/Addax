# InfluxDB2 Writer

InfluxDB2 Writer plugin implements writing data to InfluxDB 2.0 and above versions.

## Configuration Example

This plugin is used to write data to InfluxDB 2.0+ database. For detailed configuration and parameters, please refer to the original InfluxDB2 Writer documentation.

```json
--8<-- "jobs/influxdb2writer.json"
```

## Parameters

This plugin supports writing time series data to InfluxDB 2.0+ with token-based authentication and organization/bucket structure.