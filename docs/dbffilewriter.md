# DataX DbfFileWriter 说明


------------

## 1 快速介绍

DbfFileWriter提供了向本地文件写入类dbf格式的一个或者多个表文件。DbfFileWriter服务的用户主要在于DataX开发、测试同学。

**写入本地文件内容存放的是一张dbf表，例如dbf格式的文件信息。**


## 2 功能与限制

件实现了从DataX协议转为本地dbf文件功能，本地文件本身是结构化数据存储，TxtFileWriter如下几个方面约定:

1. 支持且仅支持写入 dbf的文件。

2. 支持文本压缩，现有压缩格式为gzip、bzip2。

3 支持多线程写入，每个线程写入不同子文件。

4. 文件支持滚动，当文件大于某个size值或者行数值，文件需要切换。 [暂不支持]

我们不能做到：

1. 单个文件不能支持并发写入。


## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
      "setting": {
        "speed": {
          "batchSize": 20480, 
          "bytes": -1, 
          "channel": 1
        }
      },
        "content": [
            {
        "reader": {
                "name": "streamreader",
                "parameter": {
                    "column" : [
                        {
                            "value": "DataX",
                            "type": "string"
                        },
                        {
                            "value": 19880808,
                            "type": "long"
                        },
                        {
                            "value": "1988-08-08 16:00:04",
                            "type": "date"
                        },
                        {
                            "value": true,
                            "type": "bool"
                        }
                    ],
                    "sliceRecordCount": 1000
                }
                },
           "writer": {
                     "name": "dbffilewriter", 
                     "parameter": {
                       "column": [
                         {
                           "name": "col1", 
                           "type": "char",
                           "length": 100
                         }, 
                         {
                          "name":"col2",
                          "type":"numeric",
                          "length": 18,
                          "scale": 0
                          },
                         {
                           "name": "col3", 
                           "type": "date"
                         },
                         {
                          "name":"col4",
                          "type":"logical"
                         }
                       ], 
           	          "fileName": "test.dbf",
                       "path": "/tmp/out",
                       "writeMode": "truncate"
                     }
                   }
            }
        ]
    }
}
```

### 3.2 参数说明

* **path**

	* 描述：本地文件系统的路径信息，DbfFileWriter会写入Path目录下属多个文件。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
* **column**

	* 描述：写入数据的字段，需要指定表中所有字段名和字段类型，其中：name指定字段名(长度最大为10)，type指定字段类型。 <br />

		用户可以指定Column字段信息，配置如下：

		```json
		{"column":
         [
            {
                "name": "userName",
                "type": "string"
            },
            {
                "name": "age",
                "type": "long"
            }
         ]
          }
		```

	* 必选：是 <br />

	* 默认值：无 <br />
* **fileName**

 	* 描述：DbfFileWriter写入的文件名，该文件名会添加随机的后缀作为每个线程写入实际文件名。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **writeMode**

 	* 描述：DbfFileWriter写入前数据清理处理模式： <br />

		* truncate，写入前清理目录下一fileName前缀的所有文件。
		* append，写入前不做任何处理，DataX TxtFileWriter直接使用filename写入，并保证文件名不冲突。
		* nonConflict，如果目录下有fileName前缀的文件，直接报错。

	* 必选：是 <br />

	* 默认值：无 <br />


* **compress**

	* 描述：文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、lzo、lzop、tgz、bzip2。 <br />

	* 必选：否 <br />

	* 默认值：无压缩 <br />

* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />


* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

		 例如如果用户配置: nullFormat="\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />

 	* 默认值：\N <br />

* **dateFormat**

	* 描述：日期类型的数据序列化到文件中时的格式，例如 "dateFormat": "yyyy-MM-dd"。<br />

 	* 必选：否 <br />

 	* 默认值：无 <br />

* **fileFormat(暂时支持DBASEIII)**

	* 描述：文件写出的格式，暂时只支持DBASE III。<br />

 	* 必选：否 <br />

 	* 默认值：text <br />

### 3.3 类型转换


当前该插件支持写入的类型以及对应关系如下：

| XBase Type    | XBase Symbol | Java Type used in JavaDBF |
|------------   | ------------ | ---------------------------
|Character      | C            | java.lang.String          |
|Numeric        | N            | java.math.BigDecimal      |
|Floating Point | F            | java.math.BigDecimal      |
|Logical        | L            | java.lang.Boolean         |
|Date           | D            | java.util.Date            |

其中：

* 本地文件 numeric是指本地文件中使用数字类型表示形式，例如"19901219",整形小数位数为0。
* 本地文件 logical是指本地文件文本中使用Boolean的表示形式，例如"true"、"false"。
* 本地文件 Date是指本地文件文本中使用Date表示形式，例如"2014-12-31"，Date是JAVA语言的DATE类型。


## 4 性能报告


## 5 约束限制

略

## 6 FAQ

略


