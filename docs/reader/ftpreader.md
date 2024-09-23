# Ftp Reader

Ftp Reader 提供了读取远程 FTP/SFTP 文件系统数据存储的能力。

## 功能说明

### 配置样例

=== "job/ftp2stream.json"

```json
--8<-- "jobs/ftpreader.json"
```

### 参数说明

| 配置项            | 是否必须 | 数据类型    | 默认值 | 描述                                                                          |
| :---------------- | :------: | ----------- | ------ | ----------------------------------------------------------------------------- |
| protocol          |    是    | string      | 无     | 服务器协议，目前支持传输协议有 `ftp` 和 `sftp`                                |
| host              |    是    | string      | 无     | 服务器地址                                                                    |
| port              |    否    | int         | 22/21  | 若传输协议是 `sftp` 协议，默认值是 22；若传输协议是标准 ftp 协议，默认值是 21 |
| timeout           |    否    | int         | 60000  | 连接 ftp 服务器连接超时时间，单位毫秒(ms)                                     |
| connectPattern    |    否    | string      | PASV   | 连接模式，仅支持 `PORT`, `PASV` 模式。该参数仅在 ftp 协议时使用               |
| username          |    是    | string      | 无     | ftp 服务器访问用户名                                                          |
| password          |    否    | string      | 无     | ftp 服务器访问密码                                                            |
| useKey            |    否    | boolean     | false  | 是否使用私钥登录，仅针对 sftp 登录有效                           |
| keyPath           |    否    | string      | `~/.ssh/id_rsa` | 私钥地址                                                         |
| keyPass           |    否    | string      | 无              | 私钥密码，若没有设置私钥密码，则无需配置该项                     |
| path              |    是    | list        | 无     | 远程 FTP 文件系统的路径信息，注意这里可以支持填写多个路径，详细描述见下       |
| column            |    是    | `list<map>` | 无     | 读取字段列表，type 指定源数据的类型，详见下文                                 |
| fieldDelimiter    |    是    | string      | `,`    | 描述：读取的字段分隔符                                                        |
| compress          |    否    | string      | 无     | 文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为 `zip`、`gz`、`bzip2`   |
| encoding          |    否    | string      | `utf-8`  | 读取文件的编码配置                                                            |
| skipHeader        |    否    | boolean      | false  | 类 CSV 格式文件可能存在表头为标题情况，需要跳过。默认不跳过                   |
| nullFormat        |    否    | char      | `\N`   | 定义哪些字符串可以表示为 null                                                 |
| maxTraversalLevel |    否    | int       | 100    | 允许遍历文件夹的最大层数                                                      |
| csvReaderConfig   |    否    | map      | 无     | 读取 CSV 类型文件参数配置，Map 类型。不配置则使用默认值,详见下文              |

#### path

远程 FTP 文件系统的路径信息，支持填写多个路径，示例配置中演示了如何填写多个路径。

```json
{
  "path": [
    "/var/ftp/test.txt", // 读取 /var/ftp 目录下的 test.txt 文件
    "/var/tmp/*.txt", // 读取 /var/tmp 目录下所有 txt 文件
    "/public/ftp", // 读取 /public/ftp 目录下所有文件, 如果 ftp 是文件的话，则直接读取
    "/public/a??.txt" // 读取 /public 目录下所有 a 开头，后面跟两个字符，最后是 txt 结尾的文件
  ]
}
```

特别需要注意的是，Addax 会将一个作业下同步的所有 Text File 视作同一张数据表。用户必须自己保证所有的 File 能够适配同一套 schema 信息。读取文件用户必须保证为类 CSV 格式，并且提供给 Addax 权限可读。

特别需要注意的是，如果 Path 指定的路径下没有符合匹配的文件抽取，Addax 将报错。

#### column

读取字段列表，type 指定源数据的类型，index 指定当前列来自于文本第几列(以 0 开始)，value 指定当前类型为常量，不从源头文件读取数据，而是根据 value 值自动生成对应的列。

默认情况下，用户可以全部按照 String 类型读取数据，配置如下：

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
    "index": 0,
    "description": "从远程FTP文件文本第一列获取int字段"
  },
  {
    "type": "string",
    "value": "addax",
    "description": "从FtpReader内部生成alibaba的字符串字段作为当前字段"
  }
]
```

对于用户指定 Column 信息，type 必须填写，index/value 必须选择其一。

#### csvReaderConfig

常见配置：

```json
{
  "csvReaderConfig": {
    "safetySwitch": false,
    "skipEmptyRecords": false,
    "useTextQualifier": false
  }
}
```

所有配置项及默认值,配置时 csvReaderConfig 的 map 中请 **严格按照以下字段名字进行配置**：

```ini
boolean caseSensitive = true;
char textQualifier = 34;
boolean trimWhitespace = true;
boolean useTextQualifier = true;//是否使用csv转义字符
char delimiter = 44;//分隔符
char recordDelimiter = 0;
char comment = 35;
boolean useComments = false;
int escapeMode = 1;
boolean safetySwitch = true;//单列长度是否限制100000字符
boolean skipEmptyRecords = true;//是否跳过空行
boolean captureRawRecord = true;
```

### 类型转换

远程 FTP 文件本身不提供数据类型，该类型是 Addax FtpReader 定义：

| Addax 内部类型 | 远程 FTP 文件 数据类型 |
| -------------- | ---------------------- |
| Long           | Long                   |
| Double         | Double                 |
| String         | String                 |
| Boolean        | Boolean                |
| Date           | Date                   |

其中：

- Long 是指远程 FTP 文件文本中使用整形的字符串表示形式，例如 "19901219"。
- Double 是指远程 FTP 文件文本中使用 Double 的字符串表示形式，例如 "3.1415"。
- Boolean 是指远程 FTP 文件文本中使用 Boolean 的字符串表示形式，例如 "true"、"false"。不区分大小写。
- Date 是指远程 FTP 文件文本中使用 Date 的字符串表示形式，例如 "2014-12-31"，Date 可以指定 format 格式。

## 限制

1. 单个 File 支持多线程并发读取，这里涉及到单个 File 内部切分算法
2. 单个 File 在压缩情况下，从技术上无法支持多线程并发读取。
