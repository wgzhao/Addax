# DBF Writer

DBF Writer provides writing DBF-like format to one or more table files in local file system.

## Configuration Example

```json
--8<-- "jobs/dbfwriter.json"
```

## Parameters

| Configuration | Required | Data Type   | Default Value | Description                                                      |
| :------------ | :------: | ----------- | ------------- | ---------------------------------------------------------------- |
| path          | Yes      | string      | None          | File directory, note this is a folder, not a file              |
| column        | Yes      | `list<map>` | None          | Collection of columns to be synchronized in configured table, see example configuration |
| fileName      | Yes      | string      | None          | Name of file to write                                            |
| writeMode     | Yes      | string      | None          | Data cleanup processing mode before writing, see description below |
| encoding      | No       | string      | UTF-8         | File encoding, such as `GBK`, `UTF-8`                           |
| nullFormat    | No       | string      | `\N`          | Define which string can represent null                          |
| dateFormat    | No       | string      | None          | Format when date type data is serialized to file, e.g. `"yyyy-MM-dd"` |

### writeMode

Data cleanup processing mode before writing:

- truncate: Clean all files with `fileName` prefix under directory before writing
- append: No processing before writing, write directly using `filename` and ensure no filename conflicts
- nonConflict: If there are files with `fileName` prefix under directory, report error directly

## Type Conversion

Currently this plugin supports the following write types and corresponding relationships:

| XBase Type    | XBase Symbol | Java Type used in JavaDBF |
|-------------- | ------------ | ---------------------------|
|Character      | C            | java.lang.String           |
|Numeric        | N            | java.math.BigDecimal       |
|Floating Point | F            | java.math.BigDecimal       |
|Logical        | L            | java.lang.Boolean          |