# DbfFileWriter 插件文档

## 1 快速介绍

DbfFileWriter提供了向本地文件写入类dbf格式的一个或者多个表文件。DbfFileWriter服务的用户主要在于DataX开发、测试同学。

写入本地文件内容存放的是一张dbf表，例如dbf格式的文件信息。

## 2 功能与限制

件实现了从DataX协议转为本地dbf文件功能，本地文件本身是结构化数据存储，DbfFileWriter如下几个方面约定:

1. 支持且仅支持写入dbf的文件。

2. 支持文本压缩，现有压缩格式为gzip、bzip2。

3. 支持多线程写入，每个线程写入不同子文件。

我们不能做到：

1. 单个文件不能支持并发写入。

## 3 功能说明

### 3.1 配置样例

```json
{
"job": {
  "setting": {
    "speed": {
      "batchSize": 20480,
      "bytes": -1,
      "channel": 1
      }
  },
  "content": [{
    "reader": {
      "name": "streamreader",
      "parameter": {
          "column" : [
              {
                  "value": "DataX",
                  "type": "string"
              },
              {
                  "value": 19880808,
                  "type": "long"
              },
              {
                  "value": "1988-08-08 16:00:04",
                  "type": "date"
              },
              {
                  "value": true,
                  "type": "bool"
              }
          ],
          "sliceRecordCount": 1000
          }
    },
  "writer": {
            "name": "dbffilewriter",
            "parameter": {
              "column": [
                {
                  "name": "col1",
                  "type": "char",
                  "length": 100
                },
                {
                "name":"col2",
                "type":"numeric",
                "length": 18,
                "scale": 0
                },
                {
                  "name": "col3",
                  "type": "date"
                },
                {
                "name":"col4",
                "type":"logical"
                }
              ],
            "fileName": "test.dbf",
              "path": "/tmp/out",
              "writeMode": "truncate"
            }
          }
      }
  ]}
}
```

### 3.2 参数说明

| 配置项           | 是否必须 | 默认值       |    描述    |
| :--------------- | :------: | ------------ |-------------|
| path             |    是    | 无           | DBF文件目录，注意这里是文件夹，不是文件 |
| column           |    是    | 类型默认为String  | 所配置的表中需要同步的列集合, 是 `{type: value}` 或 `{type: index}` 的集合 |
| fileName        | 是     | 无  | DbfFileWriter写入的文件名 |
| writeMode       | 是     | 无  | DbfFileWriter写入前数据清理处理模式，支持 `truncate`, `append`, `nonConflict` 三种模式，详见如下 |
| compress         | 否       | 无       | 文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2  |
| encoding            |    否    | UTF-8         | DBF文件编码，比如 `GBK`, `UTF-8` |
| nullFormat   |    否    | `\N`         | 定义哪个字符串可以表示为null, |
| dateFormat |  否   |  无  |  日期类型的数据序列化到文件中时的格式，例如 `"dateFormat": "yyyy-MM-dd"` |
| fileFormat |    否    | 无 | 文件写出的格式，暂时只支持DBASE III |  

#### writeMode

DbfFileWriter写入前数据清理处理模式：

- truncate，写入前清理目录下一fileName前缀的所有文件。
- append，写入前不做任何处理，DataX DbfFileWriter直接使用filename写入，并保证文件名不冲突。
- nonConflict，如果目录下有fileName前缀的文件，直接报错。

### 3.3 类型转换

当前该插件支持写入的类型以及对应关系如下：

| XBase Type    | XBase Symbol | Java Type used in JavaDBF |
|------------   | ------------ | ---------------------------
|Character      | C            | java.lang.String          |
|Numeric        | N            | java.math.BigDecimal      |
|Floating Point | F            | java.math.BigDecimal      |
|Logical        | L            | java.lang.Boolean         |
|Date           | D            | java.util.Date            |

其中：

- numeric 是指本地文件中使用数字类型表示形式，例如"19901219",整形小数位数为0。
- logical 是指本地文件文本中使用Boolean的表示形式，例如"true"、"false"。
- Date 是指本地文件文本中使用Date表示形式，例如"2014-12-31"，Date是JAVA语言的DATE类型。
