# DBF Writer

Dbf Writer 提供了向本地文件写入类dbf格式的一个或者多个表文件。

## 配置样例

```json
--8<-- "jobs/dbfwriter.json"
```

## 参数说明

| 配置项     | 是否必须 | 数据类型    | 默认值 | 描述                                                      |
| :--------- | :------: | ----------- | ------ | --------------------------------------------------------- |
| path       |    是    | string      | 无     | 文件目录，注意这里是文件夹，不是文件                   |
| column     |    是    | `list<map>` | 无     | 所配置的表中需要同步的列集合，详见示例配置                |
| fileName   |    是    | string      | 无     | 写入的文件名                                              |
| writeMode  |    是    | string      | 无     | 写入前数据清理处理模式，详见下面描述                      |
| encoding   |    否    | string      | UTF-8  | 文件编码，比如 `GBK`, `UTF-8`                             |
| nullFormat |    否    | string      | `\N`   | 定义哪个字符串可以表示为null,                             |
| dateFormat |    否    | string      | 无     | 日期类型的数据序列化到文件中时的格式，例如 `"yyyy-MM-dd"` |

### writeMode

写入前数据清理处理模式：

- truncate: 写入前清理目录下 `fileName` 前缀的所有文件。
- append: 写入前不做任何处理，直接使用 `filename` 写入，并保证文件名不冲突。
- nonConflict: 如果目录下有 `fileName` 前缀的文件，直接报错。

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
