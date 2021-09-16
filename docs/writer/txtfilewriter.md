# TxtFile Writer

TxtFileWriter提供了向本地文件写入类CSV格式的一个或者多个表文件。

## 配置样例

```json
--8<-- "jobs/txtwriter.json"
```

## 参数说明

| 配置项         | 是否必须 | 默认值         | 描述                                                                                 |
| :------------- | :------: | -------------- | ------------------------------------------------------------------------------------ |
| path           |    是    | 无             | 本地文件系统的路径信息，写入Path目录下属多个文件                                     |
| fileName       |    是    | 无             | 写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名                     |
| writeMode      |    是    | 无             | FtpWriter写入前数据清理处理模式，支持 `truncate`, `append`, `nonConflict` ，详见下文 |
| column         |    是    | 默认String类型 | 读取字段列表，type指定源数据的类型，详见下文                                         |
| fieldDelimiter |    是    | `,`            | 描述：读取的字段分隔符                                                               |
| compress       |    否    | 无             | 文本压缩类型，默认不压缩,支持压缩类型为 zip、lzo、lzop、tgz、bzip2                   |
| encoding       |    否    | utf-8          | 读取文件的编码配置                                                                   |
| nullFormat     |    否    | `\N`           | 定义哪些字符串可以表示为null                                                         |
| dateFormat     |    否    | 无             | 日期类型的数据序列化到文件中时的格式，例如 `"dateFormat": "yyyy-MM-dd"`              |
| fileFormat     |    否    | text           | 文件写出的格式，包括csv, text两种, 详见下文                                          |
| header         |    否    | 无             | text写出时的表头，示例 `['id', 'name', 'age']`                                       |

### writeMode

写入前数据清理处理模式：

- truncate，写入前清理目录下一fileName前缀的所有文件。
- append，写入前不做任何处理，直接使用filename写入，并保证文件名不冲突。
- nonConflict，如果目录下有fileName前缀的文件，直接报错。

### fileFormat

文件写出的格式，包括 csv 和 text 两种，csv是严格的csv格式，如果待写数据包括列分隔符，则会按照csv的转义语法转义，转义符号为双引号 `"`； text格式是用列分隔符简单分割待写数据，对于待写数据包括列分隔符情况下不做转义。

## 类型转换

| Addax 内部类型 | 本地文件 数据类型 |
| -------------- | ----------------- |
|                |
| Long           | Long              |
| Double         | Double            |
| String         | String            |
| Boolean        | Boolean           |
| Date           | Date              |
