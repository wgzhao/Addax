# 快速使用

## 安装 Addax

### 一键安装

如果你不想编译，你可以执行下面的命令，一键安装（当前仅支持 Linux 和 macOS ）

```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/wgzhao/Addax/master/install.sh)"
```

如果是 macOS ，默认安装在 `/usr/local/addax` 目录下， 如果是 Linux， 则安装在 `/opt/addax` 目录下

### 源代码编译安装

你可以选择从源代码编译安装，基本操作如下：

```shell
git clone https://github.com/wgzhao/addax.git
cd addax
mvn clean package
mvn package assembly:single
cd target/addax/addax-<version>
```

## 开始第一个采集任务

要使用 `Addax` 进行数据采集，只需要编写一个任务采集文件，该文件为 JSON 格式，以下是一个简单的配置文件，该任务的目的是从内存读取读取指定内容的数据，并将其打印出来，文件保存在 `job/test.json` 中

=== "job/test.json"

    ```json
    --8<-- "jobs/quickstart.json"
    ```

将上述文件保存为 `job/test.json`

然后执行下面的命令：

```shell
bin/addax.sh job/test.json
```

如果没有报错，应该会有类似这样的输出

```shell
--8<-- "output/quickstart.txt"
```
