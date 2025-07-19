# InfluxDB Reader

InfluxDBReader plugin implements reading data from [InfluxDB](https://www.influxdata.com). The underlying implementation calls InfluxQL language to query tables and get returned data.

## Example

The following example demonstrates how this plugin reads data from specified tables (i.e., metrics) and outputs to terminal

### Create Required Database Tables and Data

Use the following commands to create tables and data to be read:

```bash
# create database
influx --execute "CREATE DATABASE NOAA_water_database"
# download sample data
curl https://s3.amazonaws.com/noaa.water-database/NOAA_data.txt -o NOAA_data.txt
# import data via influx-cli
influx -import -path=NOAA_data.txt -precision=s -database=NOAA_water_database
```

### Create Job File

Create `job/influxdb2stream.json` file with the following content:

=== "job/influxdb2stream.json"

```json
--8<-- "jobs/influxdbreader.json"
```

### Run

Execute the following command for data collection

```bash
bin/addax.sh job/influxdb2stream.json
```

## Parameters

| Configuration | Required | Data Type | Default Value | Description                                                                        |
| :------------ | :------: | --------- | ------------- | ---------------------------------------------------------------------------------- |
| endpoint      | Yes      | string    | None          | InfluxDB connection string                                                         |
| username      | Yes      | string    | None          | Username of data source                                                            |
| password      | No       | string    | None          | Password for specified username of data source                                     |
| database      | Yes      | string    | None          | Database specified by data source                                                  |
| table         | Yes      | string    | None          | Selected table name to be synchronized                                             |
| column        | Yes      | list      | None          | Collection of column names to be synchronized in configured table, detailed description see [rdbmreader][1] |
| connTimeout   | No       | int       | 15            | Set connection timeout value, in seconds                                           |
| readTimeout   | No       | int       | 20            | Set read timeout value, in seconds                                                 |
| writeTimeout  | No       | int       | 20            | Set write timeout value, in seconds                                                |
| where         | No       | string    | None          | Filtering conditions for the table                                                 |
| querySql      | No       | string    | None          | Use SQL query to get data, if configured, `table` and `column` configuration items are invalid |

## Type Conversion

Current implementation treats all fields as strings.

## Limitations

1. Current plugin only supports version 1.x, version 2.0 and above are not supported

[1]: ../rdbmsreader