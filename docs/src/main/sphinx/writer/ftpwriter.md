# FtpWriter 插件文档

## 1 快速介绍

FtpWriter提供了向远程FTP文件写入CSV格式的一个或者多个文件，在底层实现上，FtpWriter将DataX传输协议下的数据转换为csv格式，并使用FTP相关的网络协议写出到远程FTP服务器。

写入FTP文件内容存放的是一张逻辑意义上的二维表，例如CSV格式的文本信息。

## 2 功能与限制

FtpWriter实现了从DataX协议转为FTP文件功能，FTP文件本身是无结构化数据存储，FtpWriter如下几个方面约定:

1. 支持且仅支持写入文本类型(不支持BLOB如视频数据)的文件，且要求文本中shema为一张二维表。
2. 支持类CSV格式文件，自定义分隔符。
3. 写出时不支持文本压缩。
4. 支持多线程写入，每个线程写入不同子文件。

我们不能做到：

1. 单个文件不能支持并发写入。

## 3 功能说明

### 3.1 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2,
        "bytes": -1
      }
    },
    "content": [
      {
        "reader": {},
        "writer": {
          "name": "ftpwriter",
          "parameter": {
            "protocol": "sftp",
            "host": "***",
            "port": 22,
            "username": "xxx",
            "password": "xxx",
            "timeout": "60000",
            "connectPattern": "PASV",
            "path": "/tmp/data/",
            "fileName": "yixiao",
            "writeMode": "truncate|append|nonConflict",
            "fieldDelimiter": ",",
            "encoding": "UTF-8",
            "nullFormat": "null",
            "dateFormat": "yyyy-MM-dd",
            "fileFormat": "csv",
            "header": []
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

| 配置项            | 是否必须 | 默认值 | 描述                                                                                                                |
| :---------------- | :------: | ------ | ------------------------------------------------------------------------------------------------------------------- |
| protocol          |    是    | 无     | ftp服务器协议，目前支持传输协议有ftp和sftp                                                                          |
| host              |    是    | 无     | ftp服务器地址                                                                                                       |
| port              |    否    | 22/21  | 若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21                                               |
| timeout           |    否    | 60000  | 连接ftp服务器连接超时时间，单位毫秒(ms)                                                                             |
| connectPattern    |    否    | PASV   | 连接模式，仅支持 `PORT`, `PASV` 模式。该参数只在传输协议是标准ftp协议时使用 ｜                                      |
| username          |    是    | 无     | ftp服务器访问用户名                                                                                                 |
| password          |    是    | 无     | ftp服务器访问密码                                                                                                   |
| path              |    是    | 无     | 远程FTP文件系统的路径信息，FtpWriter会写入Path目录下属多个文件                                                      |
| fileName          |    是    | 无     | FtpWriter写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名                                           |
| writeMode         |    是    | 无     | FtpWriter写入前数据清理处理模式，支持 `truncate`, `append`, `nonConflict` ，详见下文                                |
| fieldDelimiter    |    是    | `,`    | 描述：读取的字段分隔符                                                                                              |
| compress          |    否    | 无     | 文本压缩类型，暂不支持                                                                                              |
| encoding          |    否    | utf-8  | 读取文件的编码配置                                                                                                  |
| dateFormat        |    否    | 无     | 日期类型的数据序列化到文件中时的格式，例如 `"dateFormat": "yyyy-MM-dd"`                                             |
| fileFormat        |    否    | text   | 文件写出的格式，包括csv, text两种，                                                                                 |
| header            |    否    | 无     | text写出时的表头，示例 `['id', 'name', 'age']`                                                                      |
| nullFormat        |    否    | `\N`   | 定义哪些字符串可以表示为null                                                                                        |
| maxTraversalLevel |    否    | 100    | 允许遍历文件夹的最大层数                                                                                            |
| csvReaderConfig   |    否    | 无     | 读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值,详见下文 |

#### writeMod

描述：FtpWriter写入前数据清理处理模式：

1. `truncate`，写入前清理目录下一fileName前缀的所有文件。
2. `append`，写入前不做任何处理，DataX FtpWriter直接使用filename写入，并保证文件名不冲突。
3. `nonConflict`，如果目录下有fileName前缀的文件，直接报错。

### 3.3 类型转换

FTP文件本身不提供数据类型，该类型是DataX FtpWriter定义：

| DataX 内部类型 | FTP文件 数据类型            |
| -------------- | --------------------------- |
| Long           | Long -> 字符串序列化表示    |
| Double         | Double -> 字符串序列化表示  |
| String         | String -> 字符串序列化表示  |
| Boolean        | Boolean -> 字符串序列化表示 |
| Date           | Date -> 字符串序列化表示    |

其中：

- Long 是指FTP文件文本中使用整形的字符串表示形式，例如"19901219"。
- Double 是指FTP文件文本中使用Double的字符串表示形式，例如"3.1415"。
- Boolean 是指FTP文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
- Date 是指FTP文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。