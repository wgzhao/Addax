# Postgresql Writer

Postgresql Writer plugin implements the functionality of writing data to [PostgreSQL](https://postgresql.org) database tables.

## Example

The following configuration demonstrates reading data from a specified PostgreSQL table and inserting it into another table with the same table structure, to test the data types supported by this plugin.

### Table Structure Information

Assume the table creation statement and input insertion statement are as follows:

```sql
--8<-- "sql/postgresql.sql"
```

The statement to create the table to be inserted is as follows:

```sql
create table addax_tbl1 as select * from  addax_tbl where 1=2;
```

### Task Configuration

The following is the configuration file

=== "job/pg2pg.json"

```json
--8<-- "jobs/pgwriter.json"
```

Save the above configuration file as `job/pg2pg.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/pg2pg.json
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer.

### writeMode

By default, `insert into` syntax is used to write to PostgreSQL tables. If you want to use the mode of updating when primary key exists and inserting when it doesn't exist, you can use `update` mode. Assuming the table's primary key is `id`, the `writeMode` configuration method is as follows:

```json
"writeMode": "update(id)"
```

If it's a composite unique index, the configuration method is as follows:

```json
"writeMode": "update(col1, col2)"
```

Note: `update` mode was first added in version `3.1.6`, previous versions do not support it.

## Type Conversion

Currently PostgresqlWriter supports most PostgreSQL types, but there are also some cases that are not supported. Please check your types carefully.

The following lists PostgresqlWriter's type conversion list for PostgreSQL:

| Addax Internal Type | PostgreSQL Data Type                                      |
| ------------------- | --------------------------------------------------------- |
| Long                | bigint, bigserial, integer, smallint, serial             |
| Double              | double precision, money, numeric, real                   |
| String              | varchar, char, text, bit, inet,cidr,macaddr,uuid,xml,json |
| Date                | date, time, timestamp                                     |
| Boolean             | bool                                                      |
| Bytes               | bytea                                                     |

## Known Limitations

Except for the data types listed above, other data types are theoretically converted to string type, but accuracy is not guaranteed.