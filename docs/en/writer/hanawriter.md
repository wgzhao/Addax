# HANA Writer

HANA Writer plugin implements the functionality of writing data to [SAP HANA](https://www.sap.com/products/hana.html) destination tables.

## Example

Assume the HANA table to be written has the following DDL statement:

```sql
create table system.addax_tbl
(
col1 varchar(200) ,
col2 int(4),
col3 date,
col4 boolean,
col5 clob
);
```

Here we use data generated from memory to import into HANA.

=== "job/hanawriter.json"

```json
--8<-- "jobs/hanawriter.json"
```

Save the above configuration file as `job/hana2stream.json`

### Execute Collection Command

Execute the following command for data collection

```shell
bin/addax.sh job/hana2stream.json
```

## Parameters

This plugin is based on [RDBMS Writer](../rdbmswriter), so you can refer to all configuration items of RDBMS Writer.