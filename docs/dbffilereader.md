# DataX DbfFileReader 说明

## 1 快速介绍

DbfFileReader。在底层实现上，DbfFileReader，并转换为DataX传输协议传递给Writer。

**本地文件内容存放的是一张逻辑意义上的二维表，例如dbf格式的文件信息。**

## 2 功能与限制

DbfFileReader，本地文件本身是无结构化数据存储，对于DataX而言，DbfFileReader，有诸多相似之处。目前DbfFileReader支持功能如下：

1. 支持且仅支持读取dbf的文件。

2. 支持递归读取、支持文件名过滤。

3. 支持文件压缩，现有压缩格式为zip、gzip、bzip2。

4. 多个File可以支持并发读取。

我们暂时不能做到：

1. 单个DBF支持多线程并发读取，这里涉及到单个DBF内部切分算法。二期考虑支持。

2. 单个DBF在压缩情况下，从技术上无法支持多线程并发读取。

## 3 功能说明

### 3.1 配置样例

```json
{
"job": {
    "setting": {
        "speed": {
            "channel": 2
        }
    },
    "content": [
        {
            "reader": {
                "name": "dbffilereader",
                "parameter": {
                    "column": [
                        {
                            "index": 0,
                            "type": "string"
                        },
                        {
                            "index": 1,
                            "type": "string"
                        },
                        {
                            "index": 2,
                            "type": "string"
                        },
                        {
                            "index": 3,
                            "type": "string"
                        },
                        {
                            "index": 4,
                            "type": "string"
                        },
                        {
                            "value": "201908",
                            "type": "string"
                        },
                        {
                            "value": "dbf",
                            "type": "string"
                        }
                    ],
                    "path": "/tmp/test.dbf",
                    "encoding": "GBK"
                }
            },
            "writer": {
                "name": "streamwriter",
                "parameter": {
                    "print": "true"
                }
            }
    }
    ]
}
}
```

### 3.2 参数说明

| 配置项           | 是否必须 | 默认值       |    描述    |
| :--------------- | :------: | ------------ |-------------|
| path             |    是    | 无           | DBF文件路径，支持写多个路径，详细情况见下 |
| column           |    是    | 类型默认为String           | 所配置的表中需要同步的列集合, 是 `{type: value}` 或 `{type: index}` 的集合，详细配置见下 |
| compress         | 否       | 无       | 文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2  |
| encoding            |    否    | UTF-8         | DBF文件编码，比如 `GBK`, `UTF-8` |
| nullFormat   |    否    | `\N`         | 定义哪个字符串可以表示为null, |
| dbversion |    否    | 无 | 指定DBF文件版本，不指定则自动猜测 |

#### path

描述：本地文件系统的路径信息，注意这里可以支持填写多个路径。 

- 当指定单个本地文件，DbfFileReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。  
- 当指定多个本地文件，DbfFileReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。  
- 当指定通配符，DbfFileReader尝试遍历出多个文件信息。例如: 指定 `/*` 代表读取/目录下所有的文件，指定 `/bazhen/*` 代表读取bazhen目录下游所有的文件。
dbfFileReader目前只支持 `*` 作为文件通配符。

特别需要注意的是，DataX会将一个作业下同步的所有dbf File视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。读取文件用户必须保证为类dbf格式，并且提供给DataX权限可读。

特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错。

### column

读取字段列表，type指定源数据的类型，name为字段名,长度最大8，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。

默认情况下，用户可以全部按照String类型读取数据，配置如下：

```json
"column": ["*"]
```

用户可以指定Column字段信息，配置如下：

```json
{
    "type": "long",
    "index": 0    //从本地DBF文件第一列获取int字段
},
{
    "type": "string",
    "value": "alibaba"  //从dbfFileReader内部生成alibaba的字符串字段作为当前字段
}
```

对于用户指定Column信息，type必须填写，index/value必须选择其一。

### 3.3 类型转换

本地文件本身提供数据类型，该类型是DataX dbfFileReader定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

- Long 是指本地文件文本中使用整形的字符串表示形式，例如"19901219"。
- Double 是指本地文件文本中使用Double的字符串表示形式，例如"3.1415"。
- Boolean 是指本地文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
- Date 是指本地文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。

## 4 性能报告

## 5 约束限制

略

## 6 FAQ

略
