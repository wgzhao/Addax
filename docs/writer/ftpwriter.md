# Ftp Writer

Ftp Writer 提供了向远程 FTP/SFTP 服务写入文件的能力，当前仅支持写入文本文件。

## 配置样例

=== "job/stream2ftp.json"

  ```json
  --8<-- "jobs/ftpwriter.json"
  ```

## 参数说明

| 配置项            | 是否必须 | 数据类型 | 默认值          | 描述                                                             |
| :---------------- | :------: | -------- | --------------- | ---------------------------------------------------------------- |
| protocol          |    是    | string   | `ftp`           | 服务器协议，目前支持传输协议有 ftp 和 sftp                       |
| host              |    是    | string   | 无              | 服务器地址                                                       |
| port              |    否    | int      | 22/21           | ftp 默认为 21，sftp 默认为 22                                    |
| timeout           |    否    | int      | `60000`         | 连接ftp服务器连接超时时间，单位毫秒(ms)                          |
| connectPattern    |    否    | string   | `PASV`          | 连接模式，仅支持 `PORT`, `PASV` 模式。ftp协议时使用 ｜           |
| username          |    是    | string   | 无              | 用户名                                                           |
| password          |    是    | string   | 无              | 访问密码                                                         |
| useKey            |    否    | boolean  | false           | 是否使用私钥登录，仅针对 sftp 登录有效                           |
| keyPath           |    否    | string   | `~/.ssh/id_rsa` | 私钥地址                                                         |
| keyPass           |    否    | string   | 无              | 私钥密码，若没有设置私钥密码，则无需配置该项                     |
| path              |    是    | string   | 无              | 远程FTP文件系统的路径信息，FtpWriter会写入Path目录下属多个文件   |
| fileName          |    是    | string   | 无              | 写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名 |
| writeMode         |    是    | string   | 无              | 写入前数据清理处理模式，详见下文                                 |
| fieldDelimiter    |    是    | string   | `,`             | 描述：读取的字段分隔符                                           |
| compress          |    否    | string   | 无              | 文本压缩类型，暂不支持                                           |
| encoding          |    否    | string   | `utf-8`         | 读取文件的编码配置                                               |
| dateFormat        |    否    | string   | 无              | 日期类型的数据序列化到文件中时的格式，例如 `"yyyy-MM-dd"`        |
| fileFormat        |    否    | string   | `text`          | 文件写出的格式，包括 csv, text 两种，                            |
| header            |    否    | list     | 无              | text写出时的表头，示例 `['id', 'name', 'age']`                   |
| nullFormat        |    否    | string   | `\N`            | 定义哪些字符串可以表示为null                                     |
| maxTraversalLevel |    否    | int      | 100             | 允许遍历文件夹的最大层数                                         |
| csvReaderConfig   |    否    | map      | 无              | 读取CSV类型文件参数配置，详见下文                                |

### writeMode

描述：FtpWriter写入前数据清理处理模式：

1. `truncate`，写入前清理目录下一fileName前缀的所有文件。
2. `append`，写入前不做任何处理，Addax FtpWriter直接使用filename写入，并保证文件名不冲突。
3. `nonConflict`，如果目录下有fileName前缀的文件，直接报错。

### 认证

从 `4.0.2` 版本开始， 支持私钥认证方式登录 SFTP 服务器，如果密码和私有都填写了，则两者认证方式都会尝试。
注意，如果填写了 `keyPath`, `keyPass` 项，但 `useKey` 设置为 `false` ，插件依然不会尝试用私钥进行登录。

## 类型转换

FTP文件本身不提供数据类型，该类型是 Addax FtpWriter 定义：

| Addax 内部类型 | FTP文件 数据类型            |
| -------------- | --------------------------- |
| Long           | Long -> 字符串序列化表示    |
| Double         | Double -> 字符串序列化表示  |
| String         | String -> 字符串序列化表示  |
| Boolean        | Boolean -> 字符串序列化表示 |
| Date           | Date -> 字符串序列化表示    |
