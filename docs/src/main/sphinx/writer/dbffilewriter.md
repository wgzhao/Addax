# DbfFile Writer

DbfFileWriter提供了向本地文件写入类dbf格式的一个或者多个表文件。

## 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "bytes": -1,
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "Addax",
                "type": "string"
              },
              {
                "value": 19880808,
                "type": "long"
              },
              {
                "value": "1989-06-04 00:00:00",
                "type": "date"
              },
              {
                "value": true,
                "type": "bool"
              },
              {
                "value": "中文测试",
                "type": "string"
              }
            ],
            "sliceRecordCount": 10
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
                "name": "col2",
                "type": "numeric",
                "length": 18,
                "scale": 0
              },
              {
                "name": "col3",
                "type": "date"
              },
              {
                "name": "col4",
                "type": "logical"
              },
              {
                "name": "col5",
                "type": "char",
                "length": 100
              }
            ],
            "fileName": "test.dbf",
            "path": "/tmp/out",
            "writeMode": "truncate",
            "encoding": "GBK"
          }
        }
      }
    ]
  }
}
```

## 参数说明

| 配置项           | 是否必须 | 默认值       |    描述    |
| :--------------- | :------: | ------------ |-------------|
| path             |    是    | 无           | DBF文件目录，注意这里是文件夹，不是文件 |
| column           |    是    | 类型默认为String  | 所配置的表中需要同步的列集合, 是 `{type: value}` 或 `{type: index}` 的集合 |
| fileName        | 是     | 无  | DbfFileWriter写入的文件名 |
| writeMode       | 是     | 无  | DbfFileWriter写入前数据清理处理模式，支持 `truncate`, `append`, `nonConflict` 三种模式，详见如下 |
| encoding            |    否    | UTF-8         | DBF文件编码，比如 `GBK`, `UTF-8` |
| nullFormat   |    否    | `\N`         | 定义哪个字符串可以表示为null, |
| dateFormat |  否   |  无  |  日期类型的数据序列化到文件中时的格式，例如 `"dateFormat": "yyyy-MM-dd"` |

### writeMode

DbfFileWriter写入前数据清理处理模式：

- truncate，写入前清理目录下一fileName前缀的所有文件。
- append，写入前不做任何处理，Addax DbfFileWriter直接使用filename写入，并保证文件名不冲突。
- nonConflict，如果目录下有fileName前缀的文件，直接报错。

## 类型转换

当前该插件支持写入的类型以及对应关系如下：

| XBase Type    | XBase Symbol | Java Type used in JavaDBF |
|------------   | ------------ | ---------------------------
|Character      | C            | java.lang.String          |
|Numeric        | N            | java.math.BigDecimal      |
|Floating Point | F            | java.math.BigDecimal      |
|Logical        | L            | java.lang.Boolean         |
|Date           | D            | java.util.Date            |

其中：

- numeric 是指本地文件中使用数字类型表示形式，例如 `19901219` ,整形小数位数为 `0`。
- logical 是指本地文件文本中使用Boolean的表示形式，例如 `true`、`false`。
- Date 是指本地文件文本中使用Date表示形式，例如 `2014-12-31`，Date 是JAVA语言的 Date 类型。
