# TxtFile Writer

TxtFile Writer provides writing CSV-like format to one or more table files in local file system.

## Configuration Example

```json
--8<-- "jobs/txtwriter.json"
```

## Parameters

| Configuration  | Required | Data Type | Default Value | Description                                                          |
| :------------- | :------: | --------- | ------------- | -------------------------------------------------------------------- |
| path           | Yes      | string    | None          | Path information of local file system, write multiple files under Path directory |
| fileName       | Yes      | string    | None          | Name of file to write, this filename will have random suffix added as actual filename for each thread |
| writeMode      | Yes      | string    | None          | Data cleanup processing mode before writing, see below              |
| fieldDelimiter | Yes      | string    | `,`           | Field delimiter for reading                                          |
| compress       | No       | string    | None          | Text compression type, supports `zip`, `lzo`, `lzop`, `tgz`, `bzip2` |
| encoding       | No       | string    | utf-8         | Encoding configuration for reading files                            |
| nullFormat     | No       | string    | `\N`          | Define which strings can represent null                             |
| dateFormat     | No       | string    | None          | Format when date type data is serialized to file, e.g. `"yyyy-MM-dd"` |
| fileFormat     | No       | string    | text          | Format of file output, see below                                    |
| table          | Yes      | string    | None          | Table name to specify in SQL mode                                   |
| column         | No       | list      | None          | Optional column names to specify in SQL mode                        |
| extendedInsert | No       | boolean   | true          | Whether to use batch insert syntax in SQL mode, see below          |
| batchSize      | No       | int       | 2048          | Batch size for batch insert syntax in SQL mode, see below          |
| header         | No       | list      | None          | Table header for text output, example `['id', 'name', 'age']`      |

### writeMode

Data cleanup processing mode before writing:

- truncate: Clean all files with fileName prefix under directory before writing.
- append: No processing before writing, write directly using filename and ensure no filename conflicts.
- nonConflict: If there are files with fileName prefix under directory, report error directly.

### fileFormat

Format of file output, including csv, text, and sql (introduced in version `4.1.3`). CSV is strict csv format, if data to be written includes column delimiter, it will be escaped according to csv escape syntax, with escape symbol being double quotes `"`; text format simply separates data to be written with column delimiter, no escaping for data including column delimiter. SQL format means writing data to file in SQL statement (`INSERT INTO ... VALUES`) format.

### table

Only required in sql file format, used to specify the table name to write to.

### column

In sql file format, you can specify column names to write. If specified, the sql statement is like `INSERT INTO table (col1, col2, col3) VALUES (val1, val2, val3)`, otherwise it's `INSERT INTO table VALUES (val1, val2, val3)`.

### extendedInsert

Whether to enable batch insert syntax. If enabled, batchSize number of data will be written to file at once, otherwise each data is one line. This parameter borrows from the [extended-insert](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html#option_mysqldump_extended-insert) parameter syntax of `mysqldump` tool.

### batchSize

Batch size for batch insert syntax. If extendedInsert is true, every batchSize data will be written to file at once, otherwise each data is one line.

## Type Conversion

| Addax Internal Type | Local File Data Type |
| ------------------- | -------------------- |
| Long                | Long                 |
| Double              | Double               |
| string              | string               |
| Boolean             | Boolean              |
| Date                | Date                 |