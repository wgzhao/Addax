# DataX DbfFileReader 说明


------------

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

2.  单个DBF在压缩情况下，从技术上无法支持多线程并发读取。


## 3 功能说明


### 3.1 配置样例

```json
{
    "setting": {},
    "job": {
        "setting": {
            "speed": {
                "channel": 2
            }
        },
        "content": [
            { "reader": {
                                 "name": "dbffilereader",
                                 "parameter": {
                                     "column": [	
             				{"index":0, "type":"string"},
             			{"index":1, "type":"string"},
             			{"index":2, "type":"string"},
             			{"index":3, "type":"string"},
             			{"index":4, "type":"string"},
             			{"index":5, "type":"string"},
             			{"index":6, "type":"string"},
             			{"index":7, "type":"string"},
             			{"index":8, "type":"string"},
             			{"index":9, "type":"string"},
             			{"index":10, "type":"string"},
             			{"index":11, "type":"string"},
             			{"index":12, "type":"string"},
             			{"index":13, "type":"string"},
             			{"index":14, "type":"string"},
             			{"index":15, "type":"string"},
             			{"index":16, "type":"string"},
             			{"index":17, "type":"string"},
             			{"index":18, "type":"string"},
             			{"index":19, "type":"string"},
             			{"index":20, "type":"string"},
             			{"index":21, "type":"string"},
             			{"index":22, "type":"string"},
             			{"index":23, "type":"string"},
             			{"index":24, "type":"string"},
             			{"index":25, "type":"string"},
             			{"index":26, "type":"string"},
             			{"index":27, "type":"string"},
             			{"index":28, "type":"string"},
             			{"index":29, "type":"string"},
             			{"index":30, "type":"string"},
             			{"index":31, "type":"string"},
             			{"index":32, "type":"string"},
             			{"index":33, "type":"string"},
             			{"index":34, "type":"string"},
             			{"index":35, "type":"string"},
             			{"index":36, "type":"string"},
             			{"index":37, "type":"string"},
             			{"index":38, "type":"string"},
             			{"index":39, "type":"string"},
             			{"index":40, "type":"string"},
             			{"index":41, "type":"string"},
             			{"index":42, "type":"string"},
             			{"index":43, "type":"string"},
             			{"index":44, "type":"string"},
             			{"index":45, "type":"string"},
             			{"index":46, "type":"string"},
             			{"index":47, "type":"string"},
             			{"index":48, "type":"string"},
             			{"index":49, "type":"string"},
             			{"index":50, "type":"string"},
             			{"index":51, "type":"string"},
             			{"index":52, "type":"string"},
             			{"value":"201908","type":"string"},
             			{"value":"dbf","type":"string"}
             						],
             			"path": "/tmp/qtymtzl100027.dbf",
             			"encoding": "GBK"
                                 }
                             },
                "writer": {
                    "name": "hdfswriter",
                    "parameter": {
                        "column": [
				{"name": "ymth", "type":    "string"},
			{"name": "ymtzt", "type":    "string"},
			{"name": "khrq", "type":    "string"},
			{"name": "xhrq", "type":    "string"},
			{"name": "khfs", "type":    "string"},
			{"name": "khmc", "type":    "string"},
			{"name": "khlb", "type":    "string"},
			{"name": "gjdm", "type":    "string"},
			{"name": "zjlb", "type":    "string"},
			{"name": "zjdm", "type":    "string"},
			{"name": "jzrq", "type":    "string"},
			{"name": "zjdz", "type":    "string"},
			{"name": "fzzjlb", "type":    "string"},
			{"name": "fzzjdm", "type":    "string"},
			{"name": "fzjzrq", "type":    "string"},
			{"name": "fzzjdz", "type":    "string"},
			{"name": "csrq", "type":    "string"},
			{"name": "xb", "type":    "string"},
			{"name": "xldm", "type":    "string"},
			{"name": "zyxz", "type":    "string"},
			{"name": "mzdm", "type":    "string"},
			{"name": "jglb", "type":    "string"},
			{"name": "zbsx", "type":    "string"},
			{"name": "gysx", "type":    "string"},
			{"name": "jgjc", "type":    "string"},
			{"name": "ywmc", "type":    "string"},
			{"name": "gswz", "type":    "string"},
			{"name": "frxm", "type":    "string"},
			{"name": "frzjlb", "type":    "string"},
			{"name": "frzjdm", "type":    "string"},
			{"name": "lxrxm", "type":    "string"},
			{"name": "lxrzjlb", "type":    "string"},
			{"name": "lxrzjdm", "type":    "string"},
			{"name": "yddh", "type":    "string"},
			{"name": "gddh", "type":    "string"},
			{"name": "czhm", "type":    "string"},
			{"name": "lxdz", "type":    "string"},
			{"name": "lxyb", "type":    "string"},
			{"name": "dzyx", "type":    "string"},
			{"name": "dxfwbs", "type":    "string"},
			{"name": "wlfwbs", "type":    "string"},
			{"name": "cpjc", "type":    "string"},
			{"name": "cpdqr", "type":    "string"},
			{"name": "cplb", "type":    "string"},
			{"name": "glrmc", "type":    "string"},
			{"name": "glrzjlb", "type":    "string"},
			{"name": "glrzjdm", "type":    "string"},
			{"name": "tgrmc", "type":    "string"},
			{"name": "tgrzjlb", "type":    "string"},
			{"name": "tgrzjdm", "type":    "string"},
			{"name": "byzd1", "type":    "string"},
			{"name": "byzd2", "type":    "string"},
			{"name": "byzd3", "type":    "string"},
			{"name": "logdate", "type":    "string"},
			{"name": "kind", "type":    "string"}
						],
                        "compress": "SNAPPY",
                        "defaultFS": "hdfs://fzzq",
                        "fieldDelimiter": "\t",
                        "fileName": "dbf_qtymtzl",
                        "fileType": "orc",
                        "path": "/tmp/qtymtzl",
                        "writeMode": "overwrite",
			"haveKerberos": "true",
			"kerberosKeytabFilePath": "/etc/security/keytabs/hive.service.keytab",
			"kerberosPrincipal": "hive/hadoop19.fzzq.com@FZZQ.COM",
			"hadoopConfig":{
				"dfs.nameservices": "fzzq",
				"dfs.ha.namenodes.fzzq": "nn1,nn2",
				"dfs.namenode.rpc-address.fzzq.nn1": "hadoop1.fzzq.com:8020",
				"dfs.namenode.rpc-address.fzzq.nn2": "hadoop2.fzzq.com:8020",
				"dfs.client.failover.proxy.provider.fzzq": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
                    }
                    },
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": 2,
                "byte": -1,
                "record": -1,
                "batchSize": 4096
            }
        }
    }
}
```

### 3.2 参数说明

* **path**

	* 描述：本地文件系统的路径信息，注意这里可以支持填写多个路径。 <br />

		 当指定单个本地文件，DbfFileReader暂时只能使用单线程进行数据抽取。二期考虑在非压缩文件情况下针对单个File可以进行多线程并发读取。

		当指定多个本地文件，DbfFileReader支持使用多线程进行数据抽取。线程并发数通过通道数指定。

		当指定通配符，DbfFileReader尝试遍历出多个文件信息。例如: 指定/*代表读取/目录下所有的文件，指定/bazhen/\*代表读取bazhen目录下游所有的文件。**dbfFileReader目前只支持\*作为文件通配符。**

		**特别需要注意的是，DataX会将一个作业下同步的所有dbf File视作同一张数据表。用户必须自己保证所有的File能够适配同一套schema信息。读取文件用户必须保证为类dbf格式，并且提供给DataX权限可读。**

		**特别需要注意的是，如果Path指定的路径下没有符合匹配的文件抽取，DataX将报错。**

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：读取字段列表，type指定源数据的类型，name为字段名,长度最大8，value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列。 <br />

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

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />


* **compress**

	* 描述：文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2。 <br />

	* 必选：否 <br />

	* 默认值：没有压缩 <br />

* **encoding**

	* 描述：读取文件的编码配置。<br />

 	* 必选：否 <br />

 	* 默认值：utf-8 <br />


* **nullFormat**

	* 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

		 例如如果用户配置: nullFormat:"\N"，那么如果源头数据是"\N"，DataX视作null字段。

 	* 必选：否 <br />

 	* 默认值：\N <br />

* **dbversion**

	* 描述：读取dbf类型文件版本配置，string类型。读取dbf类型文件使用的dbfReader进行读取，会有很多配置，不配置则使用默认值。<br />

 	* 必选：否 <br />
 
 	* 默认值：无 <br />

        
常见配置：

```json
"csvReaderConfig":{
        "safetySwitch": false,
        "skipEmptyRecords": false,
        "useTextQualifier": false
}
```

所有配置项及默认值,配置时 dbfReaderConfig 的map中请**严格按照以下字段名字进行配置**：

```
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

### 3.3 类型转换

本地文件本身提供数据类型，该类型是DataX dbfFileReader定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
|
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：

* 本地文件 Long是指本地文件文本中使用整形的字符串表示形式，例如"19901219"。
* 本地文件 Double是指本地文件文本中使用Double的字符串表示形式，例如"3.1415"。
* 本地文件 Boolean是指本地文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* 本地文件 Date是指本地文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式。


## 4 性能报告



## 5 约束限制

略

## 6 FAQ

略


