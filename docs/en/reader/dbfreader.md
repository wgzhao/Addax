# Dbf Reader

`DbfReader` plugin supports reading DBF format files.

## Configuration

The following is a configuration example for reading DBF files and printing to terminal

=== "jobs/dbf2stream.json"

```json
--8<-- "jobs/dbfreader.json"
```

## Parameters

`parameter` configuration supports the following configurations:

| Configuration | Required | Default Value | Description                                                                              |
| :------------ | :------: | ------------- | ---------------------------------------------------------------------------------------- |
| path          | Yes      | None          | DBF file path, supports writing multiple paths, detailed description below              |
| column        | Yes      | None          | Collection of columns to be synchronized in the configured table, is a collection of `{type: value}` or `{type: index}`, detailed configuration below |
| encoding      | No       | GBK           | DBF file encoding, such as `GBK`, `UTF-8`                                               |
| nullFormat    | No       | `\N`          | Define which string can represent null                                                  |

### path

Description: Path information of local file system, note that multiple paths can be supported here.

- When specifying a single local file, DbfFileReader can currently only use single-threaded data extraction. Phase 2 considers multi-threaded concurrent reading for single files in uncompressed file situations.
- When specifying multiple local files, DbfFileReader supports using multi-threaded data extraction. Thread concurrency is specified by the number of channels.
- When specifying wildcards, DbfFileReader attempts to traverse multiple file information. For example: specifying `/*` represents reading all files under the / directory, specifying `/foo/*` represents reading all files under the `foo` directory. dbfFileReader currently only supports `*` as a file wildcard.

It is particularly important to note that Addax treats all dbf files synchronized under one job as the same data table. Users must ensure that all files can adapt to the same set of schema information. Users must ensure that the read files are in dbf-like format and provide Addax with read permissions.

It is particularly important to note that if there are no matching files for extraction under the path specified by Path, Addax will report an error.

### column

List of fields to read, `type` specifies the type of source data, `name` is the field name with maximum length of 8, `value` specifies that the current type is constant, not reading data from source file, but automatically generating corresponding columns based on `value` value.

By default, users can read all data as `String` type, configured as follows:

```json
{
  "column": ["*"]
}
```

Users can specify Column field information, configured as follows:

```json
[
  {
    "type": "long",
    "index": 0
  },
  {
    "type": "string",
    "value": "addax"
  }
]
```

- `"index": 0` means getting int field from the first column of local DBF file
- `"value": "addax"` means generating string field `addax` internally from dbfFileReader as the current field. For user-specified `column` information, `type` must be filled, and `index` and `value` must choose one.

### Supported Data Types

Local files provide data types themselves, this type is defined by Addax dbfFileReader:

| Addax Internal Type | Local File Data Type |
| ------------------- | -------------------- |
| Long                | Long                 |
| Double              | Double               |
| String              | String               |
| Boolean             | Boolean              |
| Date                | Date                 |

Where:

- Long refers to string representation of integer form in local file text, such as `19901219`.
- Double refers to string representation of Double form in local file text, such as `3.1415`.
- Boolean refers to string representation of Boolean form in local file text, such as `true`, `false`. Case insensitive.
- Date refers to string representation of Date form in local file text, such as `2014-12-31`, can configure `dateFormat` to specify format.