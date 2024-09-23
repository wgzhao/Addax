# JSON File Reader

JSON File Reader 提供了读取本地文件系统数据存储的能力。

## 配置样例

=== "job/json2stream.json"

```json
--8<-- "jobs/jsonreader.json"
```

其中 `/tmp/test*.json` 为同一个 json 文件的多个复制，内容如下：

```json
{"name": "zhangshan","id": 19890604,"age": 12,"score": {"math": 92.5,"english": 97.5,"chinese": 95},"pubdate": "2020-09-05"}
{"name": "lisi","id": 19890605,"age": 12,"score": {"math": 90.5,"english": 77.5,"chinese": 90},"pubdate": "2020-09-05"}
{"name": "wangwu","id": 19890606,"age": 12,"score": {"math": 89,"english": 100,"chinese": 92},"pubdate": "2020-09-05"}
```

## 参数说明

| 配置项         | 是否必须 | 数据类型 | 默认值 | 描述                                                                   |
| :------------- | :------: | -------- | ------ | ---------------------------------------------------------------------- |
| path           |    是    | list     | 无     | 本地文件系统的路径信息，注意这里可以支持填写多个路径,详细描述见下文    |
| column         |    是    | list     | 无     | 读取字段列表，type指定源数据的类型，详见下文                           |
| fieldDelimiter |    是    | string   | `,`    | 描述：读取的字段分隔符                                                 |
| compress       |    否    | string   | 无     | 文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2 |
| encoding       |    否    | string   | utf-8  | 读取文件的编码配置                                                     |
| singleLine     |    否    | boolean  | true  |  每条数据是否为一行， 详见下文                                   |

### path

本地文件系统的路径信息，注意这里可以支持填写多个路径，比如：

```json
{
  "path": [
    "/var/ftp/test.json", // 读取 /var/ftp 目录下的 test.json 文件
    "/var/tmp/*.json", // 读取 /var/tmp 目录下所有 json 文件
    "/public/ftp", // 读取 /public/ftp 目录下所有文件, 如果 ftp 是文件的话，则直接读取
    "/public/a??.json" // 读取 /public 目录下所有 a 开头，后面跟两个字符，最后是 json 结尾的文件
  ]
}
```

特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，Addax将报错。

### column

读取字段列表，type指定源数据的类型，index指定当前列来自于json的指定，语法为 [Jayway JsonPath](https://github.com/json-path/JsonPath) 的语法，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。 用户必须指定Column字段信息

对于用户指定Column信息，type必须填写，index/value 必须选择其一

### singleLine

使用 JSON 格式存储数据，业界有两种方式，一种是每行一个 JSON 对象，也就是 `Single Line JSON(aka. JSONL or JSON Lines)`;
另一种是整个文件是一个 JSON 数组，每个元素是一个 JSON 对象，也就是 `Multiline JSON`。

Addax 默认支持每行一个 JSON 对象的格式，即 `singeLine = true`, 在这种情况下，要注意的是：

1. 每行 JSON 对象的末尾不能有逗号，否则会解析失败。
2. 一个JSON 对象不能跨行，否则会解析失败。

如果数据是整个文件是一个 JSON 数组，每个元素是一个 JSON 对象，需要设置 `singeLine` 为 `false`。
假设上述列子中的数据用下面的格式表示：

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

因为这种格式是合法的 JSON 格式，因此每个 JSON 对象可以跨行。相应的，这类数据读取时，其 `path` 配置应该如下填写：

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

更详细的使用说明请参考 [Jayway JsonPath](https://github.com/json-path/JsonPath) 的语法。

注意: 这种数据在一个 JSON 数组里时，程序只能采取将整个文件读取到内存中，然后解析的方式，因此不适合大文件的读取。
对于大文件的读取，建议使用每行一个 JSON 对象的格式，也就是 `Single Line JSON` 的格式，这种格式可以采取逐行读取的方式，不会占用太多内存。

## 类型转换

| Addax 内部类型 | 本地文件 数据类型 |
| -------------- | ----------------- |
| Long           | Long              |
| Double         | Double            |
| String         | String            |
| Boolean        | Boolean           |
| Date           | Date              |
