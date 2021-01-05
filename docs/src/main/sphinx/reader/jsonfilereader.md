# JsonFileReader 插件文档

## 1 快速介绍

JsonFileReader 提供了读取本地文件系统数据存储的能力。在底层实现上，JsonFileReader获取本地文件数据，使用Jayway JsonPath抽取Json字符串，并转换为DataX传输协议传递给Writer。

## 2 功能与限制

JsonFileReader实现了从本地文件读取数据并转为DataX协议的功能，本地文件是可以是Json数据格式的集合，对于DataX而言，JsonFileReader实现上类比TxtFileReader，有诸多相似之处。目前JsonFileReader支持功能如下：

1. 支持且仅支持读取TXT的文件，且要求TXT中s内容必须符合json
2. 支持列常量和Json的Key为空值
3. 支持递归读取、支持文件名过滤
4. 多个File可以支持并发读取

我们暂时不能做到：

1. 单个File支持多线程并发读取，这里涉及到单个File内部切分算法。
2. 单个File在压缩情况下，从技术上无法支持多线程并发读取。
3. 暂不支持读取压缩文件和日期类型的自定义日期

## 3 功能说明

### 3.1 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,
        "bytes": -1
      }
    },
    "content": [
      {
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "print": "true"
          }
        },
        "reader": {
          "name": "jsonfilereader",
          "parameter": {
            "path": [
              "/tmp/test*.json"
            ],
            "column": [
              {
                "index": "$.id",
                "type": "long"
              },
              {
                "index": "$.name",
                "type": "string"
              },
              {
                "index": "$.age",
                "type": "long"
              },
              {
                "index": "$.score.math",
                "type": "double"
              },
              {
                "index": "$.score.english",
                "type": "double"
              },
              {
                "index": "$.pubdate",
                "type": "date"
              },
              {
                "type": "string",
                "value": "constant string"
              }
            ]
          }
        }
      }
    ]
  }
}
```

其中 `/tmp/test*.json` 为同一个 json 文件的多个复制，内容如下：

```json
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
}
```

### 3.2 参数说明

| 配置项            | 是否必须 | 默认值         | 描述                                                                   |
| :---------------- | :------: | -------------- | --------------------------------------------------------------------|
| path            |    是    | 无             | 本地文件系统的路径信息，注意这里可以支持填写多个路径,详细描述见下文                |
| column            |    是    | 默认String类型 | 读取字段列表，type指定源数据的类型，详见下文                                 |
| fieldDelimiter    |    是    | `,`            | 描述：读取的字段分隔符                                                  |
| compress          |    否    | 无             | 文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2       |
| encoding          |    否    | utf-8          | 读取文件的编码配置                                                     |

#### path

本地文件系统的路径信息，注意这里可以支持填写多个路径

- 当指定单个本地文件，JsonFileReader暂时只能使用单线程进行数据抽取。
- 当指定多个本地文件，JsonFileReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。
- 当指定通配符，JsonFileReader尝试遍历出多个文件信息。例如: 指定`/*` 代表读取/目录下所有的文件，指定`/bazhen/*` 代表读取bazhen目录下游所有的文件。 JsonFileReader目前只支持 `*` 作为文件通配符。

特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错。

#### column

读取字段列表，type指定源数据的类型，index指定当前列来自于json的指定，语法为 [Jayway JsonPath](https://github.com/json-path/JsonPath) 的语法，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。 用户必须指定Column字段信息

对于用户指定Column信息，type必须填写，index/value必须选择其一

### 3.3 类型转换

本地文件本身不提供数据类型，该类型是DataX JsonFileReade定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |
