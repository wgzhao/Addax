# JSON File Reader

JSON File Reader provides the ability to read data from local file system storage.

## Configuration Example

=== "job/json2stream.json"

```json
--8<-- "jobs/jsonreader.json"
```

Where `/tmp/test*.json` are multiple copies of the same JSON file, with content as follows:

```json
{"name": "zhangshan","id": 19890604,"age": 12,"score": {"math": 92.5,"english": 97.5,"chinese": 95},"pubdate": "2020-09-05"}
{"name": "lisi","id": 19890605,"age": 12,"score": {"math": 90.5,"english": 77.5,"chinese": 90},"pubdate": "2020-09-05"}
{"name": "wangwu","id": 19890606,"age": 12,"score": {"math": 89,"english": 100,"chinese": 92},"pubdate": "2020-09-05"}
```

## Parameters

| Configuration  | Required | Data Type | Default Value | Description                                                                     |
| :------------- | :------: | --------- | ------------- | ------------------------------------------------------------------------------- |
| path           | Yes      | list      | None          | Local file system path information, note that multiple paths can be supported  |
| column         | Yes      | list      | None          | List of fields to read, type specifies the type of source data                 |
| fieldDelimiter | Yes      | string    | `,`           | Field delimiter for reading                                                     |
| compress       | No       | string    | None          | Text compression type, default empty means no compression. Supports zip, gzip, bzip2 |
| encoding       | No       | string    | utf-8         | Encoding configuration for reading files                                       |
| singleLine     | No       | boolean   | true          | Whether each data record is on one line                                        |

### path

Local file system path information, note that multiple paths can be supported, for example:

```json
{
  "path": [
    "/var/ftp/test.json", // Read test.json file in /var/ftp directory
    "/var/tmp/*.json", // Read all json files in /var/tmp directory
    "/public/ftp", // Read all files in /public/ftp directory, if ftp is a file, read directly
    "/public/a??.json" // Read all files in /public directory starting with 'a', followed by two characters, ending with json
  ]
}
```

It is particularly important to note that if there are no matching files for extraction under the path specified by Path, Addax will report an error.

### column

List of fields to read, type specifies the type of source data, index specifies the current column from json specification using [Jayway JsonPath](https://github.com/json-path/JsonPath) syntax, value specifies that the current type is constant, not reading data from source file, but automatically generating corresponding columns based on value. Users must specify Column field information.

For user-specified Column information, type must be filled, and index/value must choose one.

### singleLine

There are two ways to store data in JSON format in the industry: one is one JSON object per line, which is `Single Line JSON (aka. JSONL or JSON Lines)`; the other is that the entire file is a JSON array, and each element is a JSON object, which is `Multiline JSON`.

Addax supports one JSON object per line by default, i.e., `singleLine = true`. In this case, note that:

1. There should be no comma at the end of each line's JSON object, otherwise parsing will fail.
2. A JSON object cannot span multiple lines, otherwise parsing will fail.

If the data is an entire file as a JSON array with each element being a JSON object, you need to set `singleLine` to `false`.
Suppose the data in the above example is represented in the following format:

```json
{
  "result": [
    {
      "name": "zhangshan",
      "id": 19890604,
      "age": 12,
      "score": {
        "math": 92.5,
        "english": 97.5,
        "chinese": 95
      },
      "pubdate": "2020-09-05"
    },
    {
      "name": "lisi",
      "id": 19890605,
      "age": 12,
      "score": {
        "math": 90.5,
        "english": 77.5,
        "chinese": 90
      },
      "pubdate": "2020-09-05"
    },
    {
      "name": "wangwu",
      "id": 19890606,
      "age": 12,
      "score": {
        "math": 89,
        "english": 100,
        "chinese": 92
      },
      "pubdate": "2020-09-05"
    }
  ]
}
```

Because this format is valid JSON format, each JSON object can span multiple lines. Correspondingly, when reading such data, its `path` configuration should be filled as follows:

```json
{
  "singleLine": false,
  "column": [
    {
      "index": "$.result[*].id",
      "type": "long"
    },
    {
      "index": "$.result[*].name",
      "type": "string"
    },
    {
      "index": "$.result[*].age",
      "type": "long"
    },
    {
      "index": "$.result[*].score.math",
      "type": "double"
    },
    {
      "index": "$.result[*].score.english",
      "type": "double"
    },
    {
      "index": "$..result[*].pubdate",
      "type": "date"
    },
    {
      "type": "string",
      "value": "constant string"
    }
  ]
}
```

For more detailed usage instructions, please refer to [Jayway JsonPath](https://github.com/json-path/JsonPath) syntax.

Note: When this type of data is in a JSON array, the program can only read the entire file into memory and then parse it, so it is not suitable for reading large files.
For reading large files, it is recommended to use the format of one JSON object per line, which is the `Single Line JSON` format. This format can be read line by line without taking up too much memory.

## Type Conversion

| Addax Internal Type | Local File Data Type |
| ------------------- | -------------------- |
| Long                | Long                 |
| Double              | Double               |
| String              | String               |
| Boolean             | Boolean              |
| Date                | Date                 |