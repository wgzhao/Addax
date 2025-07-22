# Sybase Writer

Sybase Writer plugin implements the functionality of writing data to [Sybase](https://en.wikipedia.org/wiki/Sybase) database tables.

## Configuration Example

We can use Docker container to start a Sybase database

```shell
docker run -tid --rm  -h dksybase --name sybase  -p 5000:5000  ifnazar/sybase_15_7 bash /sybase/start
```

Then create a table as follows:

```sql
create table addax_writer 
(
	id int,
	name varchar(255),
	salary float(2),
	created_at datetime,
	updated_at datetime
);
```

Then use the following task configuration file:

```json
--8<-- "jobs/sybasewriter.json"
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer.