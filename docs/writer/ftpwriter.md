# Ftp Writer

FtpWriter 提供了向远程 FTP/SFTP 服务写入文件的能力，当前仅支持写入文本文件。

## 配置样例

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
            "fileName": "test",
            "writeMode": "truncate|append|nonConflict",
            "fieldDelimiter": ",",
            "encoding": "UTF-8",
            "nullFormat": "null",
            "dateFormat": "yyyy-MM-dd",
            "fileFormat": "csv",
            "useKey": false,
            "keyPath": "",
            "keyPass": "",
            "header": []
          }
        }
      }
    ]
  }
}
```

## 参数说明

| 配置项            | 是否必须 | 默认值 | 描述                                                                                                                |
| :---------------- | :------: | ------ | ------------------------------------------------------------------------------------------------------------------- |
| protocol          |    是    | `ftp`     | ftp/sftp 服务器协议，目前支持传输协议有ftp和sftp                                                                          |
| host              |    是    | 无     | ftp/sftp 服务器地址                                                                                                       |
| port              |    否    | 22/21  | 若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21                                               |
| timeout           |    否    | `60000`  | 连接ftp服务器连接超时时间，单位毫秒(ms)                                                                             |
| connectPattern    |    否    | `PASV`   | 连接模式，仅支持 `PORT`, `PASV` 模式。该参数只在传输协议是标准ftp协议时使用 ｜                                      |
| username          |    是    | 无     | ftp/sftp 服务器访问用户名                                                                                                 |
| password          |    是    | 无     | ftp/sftp 服务器访问密码                                                                                                   |
| useKey            |    否    | false  | 是否使用私钥登录，仅针对 sftp 登录有效 |
| keyPath           |    否    | `~/.ssh/id_rsa`  | 私钥地址，如不填写，则使用默认私钥 `~/.ssh/id_rsa` |
| keyPass           |    否    | 无  | 私钥密码，若没有设置私钥密码，则无需配置该项 |
| path              |    是    | 无     | 远程FTP文件系统的路径信息，FtpWriter会写入Path目录下属多个文件                                                      |
| fileName          |    是    | 无     | FtpWriter写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名                                           |
| writeMode         |    是    | 无     | FtpWriter写入前数据清理处理模式，支持 `truncate`, `append`, `nonConflict` ，详见下文                                |
| fieldDelimiter    |    是    | `,`    | 描述：读取的字段分隔符                                                                                              |
| compress          |    否    | 无     | 文本压缩类型，暂不支持                                                                                              |
| encoding          |    否    | `utf-8`  | 读取文件的编码配置                                                                                                  |
| dateFormat        |    否    | 无     | 日期类型的数据序列化到文件中时的格式，例如 `"dateFormat": "yyyy-MM-dd"`                                             |
| fileFormat        |    否    | `text`   | 文件写出的格式，包括csv, text两种，                                                                                 |
| header            |    否    | 无     | text写出时的表头，示例 `['id', 'name', 'age']`                                                                      |
| nullFormat        |    否    | `\N`   | 定义哪些字符串可以表示为null                                                                                        |
| maxTraversalLevel |    否    | 100    | 允许遍历文件夹的最大层数                                                                                            |
| csvReaderConfig   |    否    | 无     | 读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值,详见下文 |

### writeMode

描述：FtpWriter写入前数据清理处理模式：

1. `truncate`，写入前清理目录下一fileName前缀的所有文件。
2. `append`，写入前不做任何处理，Addax FtpWriter直接使用filename写入，并保证文件名不冲突。
3. `nonConflict`，如果目录下有fileName前缀的文件，直接报错。

###  认证

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
