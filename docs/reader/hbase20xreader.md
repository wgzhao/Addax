# Hbase20XReader 

Hbase20X Reader 插件支持从 HBase 2.x 版本读取数据， 其实现方式为 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 Scan 方式读取你指定 `rowkey` 范围内的数据。

## 配置

以下演示基于下面创建的表以及数据

```shell
create 'users', {NAME=>'address', VERSIONS=>100},{NAME=>'info',VERSIONS=>1000}
put 'users', 'lisi', 'address:country', 'china1', 20200101
put 'users', 'lisi', 'address:province',    'beijing1', 20200101
put 'users', 'lisi', 'info:age',        27, 20200101
put 'users', 'lisi', 'info:birthday',   '1987-06-17', 20200101
put 'users', 'lisi', 'info:company',    'baidu1', 20200101
put 'users', 'xiaoming', 'address:city',    'hangzhou1', 20200101
put 'users', 'xiaoming', 'address:country', 'china1', 20200101
put 'users', 'xiaoming', 'address:province',    'zhejiang1',20200101
put 'users', 'xiaoming', 'info:age',        29, 20200101
put 'users', 'xiaoming', 'info:birthday',   '1987-06-17',20200101
put 'users', 'xiaoming', 'info:company',    'alibaba1', 20200101
put 'users', 'lisi', 'address:country', 'china2', 20200102
put 'users', 'lisi', 'address:province',    'beijing2', 20200102
put 'users', 'lisi', 'info:age',        27, 20200102
put 'users', 'lisi', 'info:birthday',   '1987-06-17', 20200102
put 'users', 'lisi', 'info:company',    'baidu2', 20200102
put 'users', 'xiaoming', 'address:city',    'hangzhou2', 20200102
put 'users', 'xiaoming', 'address:country', 'china2', 20200102
put 'users', 'xiaoming', 'address:province',    'zhejiang2', 20200102
put 'users', 'xiaoming', 'info:age',        29, 20200102
put 'users', 'xiaoming', 'info:birthday',   '1987-06-17', 20200102
put 'users', 'xiaoming', 'info:company',    'alibaba2', 20200102
```

### normal 模式

把HBase中的表，当成普通二维表（横表）进行读取,读取最新版本数据。如：

```shell
hbase(main):017:0> scan 'users'
ROW           COLUMN+CELL
 lisi         column=address:city, timestamp=1457101972764, value=beijing
 lisi         column=address:country, timestamp=1457102773908, value=china
 lisi         column=address:province, timestamp=1457101972736, value=beijing
 lisi         column=info:age, timestamp=1457101972548, value=27
 lisi         column=info:birthday, timestamp=1457101972604, value=1987-06-17
 lisi         column=info:company, timestamp=1457101972653, value=baidu
 xiaoming     column=address:city, timestamp=1457082196082, value=hangzhou
 xiaoming     column=address:country, timestamp=1457082195729, value=china
 xiaoming     column=address:province, timestamp=1457082195773, value=zhejiang
 xiaoming     column=info:age, timestamp=1457082218735, value=29
 xiaoming     column=info:birthday, timestamp=1457082186830, value=1987-06-17
 xiaoming     column=info:company, timestamp=1457082189826, value=alibaba
2 row(s) in 0.0580 seconds
```

读取后数据

| rowKey   | addres:city | address:country | address:province | info:age | info:birthday | info:company |
| -------- | ----------- | -------------- | ---------------- | -------- | ------------- | ------------ |
| lisi     | beijing     | china          | beijing          | 27       | 1987-06-17    | baidu        |
| xiaoming | hangzhou    | china          | zhejiang         | 29       | 1987-06-17    | alibaba      |

### multiVersionFixedColumn 模式

把HBase中的表，当成竖表进行读取。读出的每条记录一定是四列形式，依次为：`rowKey`，`family:qualifier`，`timestamp`，`value`。

读取时需要明确指定要读取的列，把每一个 cell 中的值，作为一条记录（record），若有多个版本就有多条记录（record）。如：

```shell
hbase(main):018:0> scan 'users',{VERSIONS=>5}
ROW              COLUMN+CELL
 lisi            column=address:city, timestamp=1457101972764, value=beijing
 lisi            column=address:contry, timestamp=1457102773908, value=china
 lisi            column=address:province, timestamp=1457101972736, value=beijing
 lisi            column=info:age, timestamp=1457101972548, value=27
 lisi            column=info:birthday, timestamp=1457101972604, value=1987-06-17
 lisi            column=info:company, timestamp=1457101972653, value=baidu
 xiaoming        column=address:city, timestamp=1457082196082, value=hangzhou
 xiaoming        column=address:contry, timestamp=1457082195729, value=china
 xiaoming        column=address:province, timestamp=1457082195773, value=zhejiang
 xiaoming        column=info:age, timestamp=1457082218735, value=29
 xiaoming        column=info:age, timestamp=1457082178630, value=24
 xiaoming        column=info:birthday, timestamp=1457082186830, value=1987-06-17
 xiaoming        column=info:company, timestamp=1457082189826, value=alibaba
2 row(s) in 0.0260 seconds
```

读取后数据(4列)

| rowKey   | column:qualifier | timestamp     | value      |
 | -------- | ---------------- | ------------- | ---------- |
| lisi     | address:city     | 1457101972764 | beijing    |
| lisi     | address:contry   | 1457102773908 | china      |
| lisi     | address:province | 1457101972736 | beijing    |
| lisi     | info:age         | 1457101972548 | 27         |
| lisi     | info:birthday    | 1457101972604 | 1987-06-17 |
| lisi     | info:company     | 1457101972653 | beijing    |
| xiaoming | address:city     | 1457082196082 | hangzhou   |
| xiaoming | address:contry   | 1457082195729 | china      |
| xiaoming | address:province | 1457082195773 | zhejiang   |
| xiaoming | info:age         | 1457082218735 | 29         |
| xiaoming | info:age         | 1457082178630 | 24         |
| xiaoming | info:birthday    | 1457082186830 | 1987-06-17 |
| xiaoming | info:company     | 1457082189826 | alibaba    |

配置一个从 HBase 抽取数据到本地的作业，分别为标准模式和多版本模式

=== "hbase20x2stream_normal.json"

    ```json
    --8<-- "jobs/hbase20xreader_normal.json"
    ```

=== "hbase20x2stream_version.json"

    ```json
    --8<-- "jobs/hbase20xreader_version.json"
    ```

## 参数说明

| 配置项        | 是否必须 | 默认值 | 描述                                                                                              |
| :------------ | :------: | ------ | --------------------------------------------------------------------------------------------- |
| hbaseConfig   |    是    | 无     | 连接HBase集群需要的配置信息, `hbase.zookeeper.quorum` 为必填项，其他 HBase client的配置为可选项    |
| mode          |    是    | 无     | 读取hbase的模式，可填写 `normal` 或 `multiVersionFixedColumn`                                     |
| table         |    是    | 无     | 要读取的 hbase 表名（大小写敏感）                                                                  |
| encoding      |    否    | UTF-8  | 编码方式，`UTF-8` 或是 `GBK`，用于对二进制存储的 `HBase byte[]` 转为 `String` 时的编码              |
| column        |    是    | 无     | 要读取的字段，`normal` 模式与 `multiVersionFixedColumn` 模式下必填项, 详细说明见下文                  |
| maxVersion    |    是    | 无     | 指定在多版本模式下读取的版本数，`-1` 表示读取所有版本, `multiVersionFixedColumn` 模式下必填 |
| range         |    否    | 无     | 指定读取的 `rowkey` 范围, 详见下文                                                         |
| scanCacheSize |    否    | 256    | 每次从服务器端读取的行数                                                                      |
| scanBatchSize |    否    | 100    | 每次从服务器端读取的列数                                                                  |

### column

描述：要读取的字段，`normal` 模式与 `multiVersionFixedColumn` 模式下必填项。

#### normal 模式

`name` 指定读取的 hbase 列，除了 `rowkey` 外，必须为 `列族:列名` 的格式，`type` 指定源数据的类型，`format`指定日期类型的格式，
`value` 指定当前类型为常量，不从 hbase 读取数据，而是根据 `value`  值自动生成对应的列。配置格式如下：

```json
{
  "column": [
    {
      "name": "rowkey",
      "type": "string"
    },
    {
      "value": "test",
      "type": "string"
    }
  ]
}
```

normal 模式下，对于用户指定Column信息，type必须填写，name/value必须选择其一。

#### multiVersionFixedColumn 模式

`name` 指定读取的 hbase 列，除了 `rowkey` 外，必须为 `列族:列名` 的格式，`type` 指定源数据的类型，`format`指定日期类型的格式 。
`multiVersionFixedColumn` 模式下不支持常量列。
配置格式如下：

```json
{
  "mode": "multiVersionFixedColumn",
  "maxVersion": 3,
  "column": [
    {
      "name": "rowkey",
      "type": "string"
    },
    {
      "name": "info: age",
      "type": "string"
    }
  ]
}
```

### range

指定读取的 `rowkey` 范围

- `startRowkey`：指定开始 `rowkey`
- `endRowkey`: 指定结束 `rowkey`
- `isBinaryRowkey`：指定配置的 `startRowkey` 和 `endRowkey` 转换为`byte[]`时的方式，默认值为false,若为true，则调用`Bytes.toBytesBinary(rowkey)`方法进行转换;若为false：则调用`Bytes.toBytes(rowkey)`

配置格式如下：

```json
{
  "range": {
    "startRowkey": "aaa",
    "endRowkey": "ccc",
    "isBinaryRowkey": false
  }
}
```

## 类型转换

下面列出支持的读取HBase数据类型:

| Addax 内部类型 | HBase 数据类型      |
| -------------- | ------------------- |
| Long           | int, short ,long    |
| Double         | float, double       |
| String         | string, binarystring |
| Date           | date                |
| Boolean        | boolean             |

请注意:

`除上述罗列字段类型外，其他类型均不支持`

## 限制

1. 目前不支持动态列的读取。考虑网络传输流量（支持动态列，需要先将hbase所有列的数据读取出来，再按规则进行过滤），现支持的两种读取模式中需要用户明确指定要读取的列。
2. 关于同步作业的切分：目前的切分方式是根据用户hbase表数据的region分布进行切分。即：在用户填写的 `[startrowkey，endrowkey］` 范围内，一个region会切分成一个task，单个region不进行切分。
3. `multiVersionFixedColumn` 模式下不支持增加常量列
