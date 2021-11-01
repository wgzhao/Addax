# 快速使用

## 安装 Addax

如果你不想编译，你可以执行下面的命令，直接从下载已经编译好的二进制文件

=== "直接下载二进制"

    ``` shell
    curl -sS -o addax-4.0.2.tar.gz \ 
      https://github.com/wgzhao/Addax/releases/download/4.0.2/addax-4.0.2.tar.gz`

    tar -xzf addax-4.0.2.tar.gz
    cd addax-4.0.2
    ```

=== "源代码编译"

    ``` shell
    git clone https://github.com/wgzhao/addax.git
    cd addax
    git checkout 4.0.2
    mvn clean package 
    mvn package assembly:single
    cd target/addax/addax-4.0.2
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
