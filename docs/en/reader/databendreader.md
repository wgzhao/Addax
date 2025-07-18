# Databend Reader

DatabendReader plugin implements reading data from [Databend](https://databend.rs).

Note that Databend has MySQL client protocol compatible implementation, so you can directly use [MySQL Reader](../mysqlreader) to read Databend data.

## Example

We can start Databend database in the following way

```shell
docker run  -tid  --rm  -p 8000:8000 \
   -e QUERY_DEFAULT_USER=databend \
   -e QUERY_DEFAULT_PASSWORD=databend \
   datafuselabs/databend
```

Then create a table to read

```sql
(
	id int,
	name varchar(255),
	salary float,
	created_at datetime,
	updated_at datetime
);
```

And populate necessary data

The following configuration reads this table to terminal:

=== "job/databend2stream.json"

  ```json
  --8<-- "jobs/databend2stream.json"
  ```

Save the above configuration file as `job/databend2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/databend2stream.json
```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all parameters of RDBMS Reader.

## Limitations

None at the moment