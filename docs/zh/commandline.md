# 命令行工具

这里介绍 [addax.sh](https://github.com/wgzhao/Addax/blob/master/bin/addax.sh) 命令行工具的使用方法。

## 命令行工具

Addax 提供了一个命令行工具 `addax.sh`，用于执行 Addax 作业。该工具位于 Addax 安装目录的 `bin` 目录下。

## 使用方法

执行 Addax 作业的基本命令格式如下：

```bash
bin/addax.sh <job_file>
```

其中 `<job_file>` 是一个 JSON 格式的作业配置文件，包含了数据源的读取和写入配置。

## 参数

`addax.sh` 命令行工具支持以下参数：

- `-h`, `--help`: 显示帮助信息。
- `-v`, `--version`: 显示 Addax 的版本信息。
- `-l`, `--log`: 指定日志文件路径，默认是在 `$ADDAX_HOME/log` 目录下。
- `-d`, `--debug`: 启用调试模式，详见 [调试模式](docs/debug.md)。
- `-L`, `--log-level`: 设置日志级别，支持 `DEBUG`, `INFO`, `WARN`, `ERROR` 等级别，默认是 `INFO`。
- `-j`, `--jvm`: 指定 JVM 参数，可以传递给 Java 虚拟机的参数。
- `-p`, `--params` : 传递额外的参数给作业配置文件，可以在作业配置中使用 `${param}` 的方式引用这些参数。

### params 参数说明

`-p`, `--params` 参数用来向设置某些配置任务的参数，使得作业配置文件可以更加灵活。可以在作业配置文件中使用 `${param}` 的方式引用这些参数。

假定作业配置文件中有如下内容：

```json
{
  "job": {
    "settings": {
      "param1": "${param1}",
      "param2": "${param2}"
    },
    "content": {
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "username": "${username}",
          "password": "${password}"
        }
      },
      "writer": {
        "name": "mysqlwriter",
        "parameter": {
          "username": "${username}",
          "password": "${password}"
        }
      }
    }
  }
}
```

执行命令时，可以使用 `-p` 参数传递参数：

```bash
bin/addax.sh job/test.json -p "-Dusername=root -Dpassword=123456 -Dparam1=value1 -Dparam2=value2"
```

为了方便使用，程序内置了部分常见时间变量，可以直接。假定当前时间是 `2025-07-16 12:13:14`，下面列出可以直接使用的变量以及变量值：

| 变量名                       | 变量值                   |
|---------------------------|-----------------------|
| `${curr_date_short}`      | `20250716`            |
| `${curr_date_dash}`       | `2025-07-16`          |
| `$curr_datetime_short}`   | `20250716121314`      |
| `${curr_datetime_dash}`   | `2025-07-16 12:13:14` |
| `${biz_date_short}`       | `20250715`            |
| `${biz_date_dash}`        | `2025-07-15`          |
| `${biz_datetime_short}`   | `20250715121314`      |
| `${biz_datetime_dash}`    | `2025-07-15 12:13:14` |
| `${biz_datetime_0_short}` | `20250715000000`      |
| `${biz_datetime_0_dash}`  | `2025-07-15 00:00:00` |

