# Dbf Reader

`DbfReader` 插件支持读取 DBF 格式文件

## 配置说明

以下是读取 DBF 文件后打印到终端的配置样例

=== "jobs/dbf2stream.json"

```json
--8<-- "jobs/dbfreader.json"
```

## 参数说明

`parameter` 配置项支持以下配置

| 配置项     | 是否必须 | 默认值 | 描述                                                                                     |
| :--------- | :------: | ------ | ---------------------------------------------------------------------------------------- |
| path       |    是    | 无     | DBF 文件路径，支持写多个路径，详细情况见下                                               |
| column     |    是    | 无     | 所配置的表中需要同步的列集合, 是 `{type: value}` 或 `{type: index}` 的集合，详细配置见下 |
| encoding   |    否    | GBK    | DBF 文件编码，比如 `GBK`, `UTF-8`                                                        |
| nullFormat |    否    | `\N`   | 定义哪个字符串可以表示为 null,                                                           |

### path

描述：本地文件系统的路径信息，注意这里可以支持填写多个路径。

- 当指定单个本地文件，DbfFileReader 暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个 File 可以进行多线程并发读取。
- 当指定多个本地文件，DbfFileReader 支持使用多线程进行数据抽取。线程并发数通过通道数指定。
- 当指定通配符，DbfFileReader 尝试遍历出多个文件信息。例如: 指定 `/*` 代表读取/目录下所有的文件，指定 `/foo/*` 代表读取 `foo` 目录下游所有的文件。 dbfFileReader 目前只支持 `*` 作为文件通配符。

特别需要注意的是，Addax 会将一个作业下同步的所有 dbf File 视作同一张数据表。用户必须自己保证所有的 File 能够适配同一套 schema 信息。读取文件用户必须保证为类 dbf 格式，并且提供给 Addax 权限可读。

特别需要注意的是，如果 Path 指定的路径下没有符合匹配的文件抽取，Addax 将报错。

### column

读取字段列表，`type` 指定源数据的类型，`name` 为字段名,长度最大 8，`value` 指定当前类型为常量，不从源头文件读取数据，而是根据 `value` 值自动生成对应的列。

默认情况下，用户可以全部按照 `String` 类型读取数据，配置如下：

```json
{
  "column": ["*"]
}
```

用户可以指定 Column 字段信息，配置如下：

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

- `"index": 0` 表示从本地 DBF 文件第一列获取 int 字段
- `"value": "addax"` 表示从 dbfFileReader 内部生成 `addax` 的字符串字段作为当前字段 对于用户指定 `column`信息，`type` 必须填写，`index` 和 `value` 必须选择其一。

### 支持的数据类型

本地文件本身提供数据类型，该类型是 Addax dbfFileReader 定义：

| Addax 内部类型 | 本地文件 数据类型 |
| -------------- | ----------------- |
| Long           | Long              |
| Double         | Double            |
| String         | String            |
| Boolean        | Boolean           |
| Date           | Date              |

其中：

- Long 是指本地文件文本中使用整形的字符串表示形式，例如 `19901219`。
- Double 是指本地文件文本中使用 Double 的字符串表示形式，例如 `3.1415`。
- Boolean 是指本地文件文本中使用 Boolean 的字符串表示形式，例如 `true`、`false`。不区分大小写。
- Date 是指本地文件文本中使用 Date 的字符串表示形式，例如 `2014-12-31`，可以配置 `dateFormat` 指定格式。
