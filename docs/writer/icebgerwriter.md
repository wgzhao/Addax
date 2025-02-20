# Paimon Writer

Paimon Writer 提供向 已有的paimon表写入数据的能力。

## 配置样例

```json
--8<-- "jobs/icebergwriter.json"
```

## 参数说明

| 配置项          | 是否必须 | 数据类型   | 默认值 | 说明                                              |
|:-------------|:----:|--------|----|-------------------------------------------------|
| tableName    |  是   | string | 无  | 要写入的iceberg表名                                   |
| catalogType  |  是   | string   | 无 | catalog类型, 目前支持 hive,hadoop                     |
| warehouse    |  是   | string   | 无 | 仓库地址                                            |
| writeMode    |  是   | string | 无  | 写入模式，详述见下                                       |
| hadoopConfig |  是   | json   | {} | 里可以配置与 Iceberg catalog和Hadoop 相关的一些高级参数，比如HA的配置 |



### writeMode

写入前数据清理处理模式：

- append，写入前不做任何处理，直接写入，不清除原来的数据。
- truncate 写入前先清空表，再写入。

### hadoopConfig

`hadoopConfig` 里可以配置与 Iceberg catalog和Hadoop 相关的一些高级参数，比如HA的配置

创建表实例:

依赖包设置:

build.gradle

```groovy
plugins {
    id 'java'
}

group = 'com.awol2005ex'
version = '1.0-SNAPSHOT'
ext["hadoop_version"] = "3.2.4"
ext["hive_version"] = "3.1.3"
ext["woodstox_version"] = "7.0.0"
ext["iceberg_version"] = "1.8.0"
repositories {
    maven { url "https://maven.aliyun.com/repository/central" }
    maven { url "https://maven.aliyun.com/repository/public" }
    maven {
        url 'https://repo.huaweicloud.com/repository/maven/'
    }
    maven {
        url 'https://repo.spring.io/libs-milestone/'
    }


    maven {
        url 'https://repo.spring.io/libs-snapshot'
    }
    mavenCentral()
}

dependencies {
    testImplementation platform('org.junit:junit-bom:5.10.0')
    testImplementation 'org.junit.jupiter:junit-jupiter'
    implementation("org.apache.hadoop:hadoop-common:${hadoop_version}") {
        exclude group: 'com.fasterxml.jackson.core', module: 'jackson-databind'
        exclude group: 'org.codehaus.jackson', module: 'jackson-core-asl'
        exclude group: 'org.codehaus.jackson', module: 'jackson-mapper-asl'
        exclude group: 'com.fasterxml.woodstox', module: 'woodstox-core'
        exclude group: 'commons-codec', module: 'commons-codec'
        exclude group: 'commons-net', module: 'commons-net'
        exclude group: 'io.netty', module: 'netty'
        exclude group: 'log4j', module: 'log4j'
        exclude group: 'net.minidev', module: 'json-smart'
        exclude group: 'org.codehaus.jettison', module: 'jettison'
        exclude group: 'org.eclipse.jetty', module: 'jetty-server'
        exclude group: 'org.xerial.snappy', module: 'snappy-java'
        exclude group: 'org.apache.zookeeper', module: 'zookeeper'
        exclude group: 'org.eclipse.jetty', module: 'jetty-util'
    }
    implementation("org.apache.hadoop:hadoop-aws:${hadoop_version}") {
        exclude group: 'com.fasterxml.jackson.core', module: 'jackson-databind'
        exclude group: 'org.codehaus.jackson', module: 'jackson-core-asl'
        exclude group: 'org.codehaus.jackson', module: 'jackson-mapper-asl'
        exclude group: 'com.fasterxml.woodstox', module: 'woodstox-core'
        exclude group: 'commons-codec', module: 'commons-codec'
        exclude group: 'commons-net', module: 'commons-net'
        exclude group: 'io.netty', module: 'netty'
        exclude group: 'log4j', module: 'log4j'
        exclude group: 'net.minidev', module: 'json-smart'
        exclude group: 'org.codehaus.jettison', module: 'jettison'
        exclude group: 'org.eclipse.jetty', module: 'jetty-server'
        exclude group: 'org.xerial.snappy', module: 'snappy-java'
        exclude group: 'org.apache.zookeeper', module: 'zookeeper'
        exclude group: 'org.eclipse.jetty', module: 'jetty-util'
    }
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:${hadoop_version}") {
        exclude group: 'com.fasterxml.jackson.core', module: 'jackson-databind'
        exclude group: 'org.codehaus.jackson', module: 'jackson-core-asl'
        exclude group: 'org.codehaus.jackson', module: 'jackson-mapper-asl'
        exclude group: 'com.fasterxml.woodstox', module: 'woodstox-core'
        exclude group: 'commons-codec', module: 'commons-codec'
        exclude group: 'commons-net', module: 'commons-net'
        exclude group: 'io.netty', module: 'netty'
        exclude group: 'log4j', module: 'log4j'
        exclude group: 'net.minidev', module: 'json-smart'
        exclude group: 'org.codehaus.jettison', module: 'jettison'
        exclude group: 'org.eclipse.jetty', module: 'jetty-server'
        exclude group: 'org.xerial.snappy', module: 'snappy-java'
        exclude group: 'org.apache.zookeeper', module: 'zookeeper'
        exclude group: 'org.eclipse.jetty', module: 'jetty-util'
    }
    implementation("org.apache.hive:hive-metastore:${hive_version}"){
        exclude group: 'com.fasterxml.jackson.core', module: 'jackson-databind'
        exclude group: 'org.codehaus.jackson', module: 'jackson-core-asl'
        exclude group: 'org.codehaus.jackson', module: 'jackson-mapper-asl'
        exclude group: 'com.fasterxml.woodstox', module: 'woodstox-core'
        exclude group: 'commons-codec', module: 'commons-codec'
        exclude group: 'commons-net', module: 'commons-net'
        exclude group: 'io.netty', module: 'netty'
        exclude group: 'log4j', module: 'log4j'
        exclude group: 'net.minidev', module: 'json-smart'
        exclude group: 'org.codehaus.jettison', module: 'jettison'
        exclude group: 'org.eclipse.jetty', module: 'jetty-server'
        exclude group: 'org.xerial.snappy', module: 'snappy-java'
        exclude group: 'org.apache.zookeeper', module: 'zookeeper'
        exclude group: 'org.eclipse.jetty', module: 'jetty-util'
    }
    implementation("com.fasterxml.woodstox:woodstox-core:${woodstox_version}")

    implementation("org.apache.iceberg:iceberg-common:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-api:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-arrow:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-aws:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-core:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-parquet:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-orc:${iceberg_version}")
    implementation("org.apache.iceberg:iceberg-hive-metastore:${iceberg_version}")
}

test {
    useJUnitPlatform()
}
```

创建存储在minio,catalogType是hadoop的iceberg表

```java
package com.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.io.IOException;

public class CreateMinioTable {
    public static void main(String[] args) throws IOException {

        Configuration hadoopConf = new Configuration();
        "fs.s3a.endpoint", "http://localhost:9000");
        "fs.s3a.access.key", "gy0dX5lALP176g6c9fYf");
        "fs.s3a.secret.key", "ReuUrCzzu5wKWAegtswoHIWV389BYl9AB1ZQbiKr");
        "fs.s3a.connection.ssl.enabled", "false");
        "fs.s3a.path.style.access", "true");
        "fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
        String warehousePath = "s3a://pvc-91d1e2cd-4d25-45c9-8613-6c4f7bf0a4cc/iceberg";
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehousePath);

        TableIdentifier name = TableIdentifier.of("test", "test1");

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "ts1", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "name", Types.StringType.get())
        );
        Table table = catalog.createTable(name, schema);
        System.out.println(table.location());

        catalog.close();
    }
}

```

创建存储在hdfs,catalogType是hadoop的iceberg表
```java
package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.io.IOException;

public class CreateHdfsTable {
    public static void main(String[] args) throws IOException {

        System.setProperty("java.security.krb5.conf","D:/MIT/krb5.ini");

        Configuration hadoopConf = new Configuration();
        "fs.defaultFS", "hdfs://nameservice1");
        "hadoop.security.authentication", "kerberos");
        "hadoop.kerberos.principal", "hive/_HOST@XXX.COM");
        "hadoop.kerberos.keytab", "/tmp/hive@XXX.COM.keytab");
        "ha.zookeeper.quorum", "nn1:2181,nn2:2181,nn3:2181");
        "dfs.nameservices", "nameservice1");
        "dfs.namenode.rpc-address.nameservice1.namenode371", "nn2:8020");
        "dfs.namenode.rpc-address.nameservice1.namenode265", "nn1:8020");
        "dfs.namenode.keytab.file", "/tmp/hdfs@XXX.COM.keytab");
        "dfs.namenode.keytab.enabled", "true");
        "dfs.namenode.kerberos.principal", "hdfs/_HOST@XXX.COM");
        "dfs.namenode.kerberos.internal.spnego.principal", "HTTP/_HOST@XXX.COM");
        "dfs.ha.namenodes.nameservice1", "namenode265,namenode371");
        "dfs.datanode.keytab.file", "/tmp/hdfs@XXX.COM.keytab");
        "dfs.datanode.keytab.enabled", "true");
        "dfs.datanode.kerberos.principal", "hdfs/_HOST@XXX.COM");
        "dfs.client.use.datanode.hostname", "false");
        "dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        "dfs.balancer.keytab.file", "/tmp/hdfs@XXX.COM.keytab");
        "dfs.balancer.keytab.enabled", "true");
        "dfs.balancer.kerberos.principal", "hdfs/_HOST@XXX.COM");

        UserGroupInformation.setConfiguration(hadoopConf);
        try {
            UserGroupInformation.loginUserFromKeytab("hive@XXX.COM", "/tmp/hive@XXX.COM.keytab");
        } catch (Exception e) {
            e.printStackTrace();
        }



        String warehousePath = "hdfs://nameservice1/user/hive/iceberg";
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehousePath);

        TableIdentifier name = TableIdentifier.of("test1", "test20250219");

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "ts1", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "dec1", Types.DecimalType.of(12,2)),
                Types.NestedField.required(4, "bool1", Types.BooleanType.get()),
                Types.NestedField.required(5, "map1", Types.MapType.ofRequired(11,12,Types.StringType.get(),Types.StringType.get())),
                Types.NestedField.required(6, "date1", Types.DateType.get()),
                Types.NestedField.required(7, "float1", Types.FloatType.get()),
                Types.NestedField.required(8, "double1", Types.DoubleType.get()),
                Types.NestedField.required(9, "array1", Types.ListType.ofRequired(13,Types.StringType.get())),
                Types.NestedField.required(10, "name", Types.StringType.get())
        );
        catalog.dropTable(name,true);
        Table table = catalog.createTable(name, schema);
        System.out.println(table.location());

        catalog.close();
    }
}
```
创建hive表

```java
package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CreateHiveTable {
    public static void main(String[] args) throws IOException {

        System.setProperty("java.security.krb5.conf","D:/MIT/krb5.ini");

        Configuration hadoopConf = new Configuration();
        "fs.defaultFS", "hdfs,//nameservice1");
        "hadoop.security.authentication", "kerberos");
        "hadoop.kerberos.principal", "hive/_HOST@XXX.COM");
        "hadoop.kerberos.keytab", "/tmp/hive@XXX.COM.keytab");
        "ha.zookeeper.quorum", "nn1:2181,nn2:2181,nn3:2181");
        "dfs.nameservices", "nameservice1");
        "dfs.namenode.rpc-address.nameservice1.namenode371", "nn2:8020");
        "dfs.namenode.rpc-address.nameservice1.namenode265", "nn1:8020");
        "dfs.namenode.keytab.file", "/tmp/hdfs@XXX.COM.keytab");
        "dfs.namenode.keytab.enabled", "true");
        "dfs.namenode.kerberos.principal", "hdfs/_HOST@XXX.COM");
        "dfs.namenode.kerberos.internal.spnego.principal", "HTTP/_HOST@XXX.COM");
        "dfs.ha.namenodes.nameservice1", "namenode265,namenode371");
        "dfs.datanode.keytab.file", "/tmp/hdfs@XXX.COM.keytab");
        "dfs.datanode.keytab.enabled", "true");
        "dfs.datanode.kerberos.principal", "hdfs/_HOST@XXX.COM");
        "dfs.client.use.datanode.hostname", "false");
        "dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        "dfs.balancer.keytab.file", "/tmp/hdfs@XXX.COM.keytab");
        "dfs.balancer.keytab.enabled", "true");
        "dfs.balancer.kerberos.principal", "hdfs/_HOST@XXX.COM");

        "hive.metastore.uris", "thrift://nn1:9083,thrift://nn2:9083");
        "hive.server2.authentication","kerberos");
        "hive.metastore.kerberos.principal","hive/_HOST@XXX.COM");

        "hive.metastore.sasl.enabled", "true");

        UserGroupInformation.setConfiguration(hadoopConf);
        try {
            UserGroupInformation.loginUserFromKeytab("hive@XXX.COM", "/tmp/hive@XXX.COM.keytab");
        } catch (Exception e) {
            e.printStackTrace();;
        }




        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(hadoopConf);
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", "/warehouse/tablespace/managed/hive");
        properties.put("uri", "thrift://nn1:9083,thrift://nn2:9083");

        catalog.initialize("hive", properties);

        TableIdentifier name = TableIdentifier.of("test1", "test20250218");

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "ts1", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "name", Types.StringType.get())
        );
        Table table = catalog.createTable(name, schema);
        System.out.println(table.location());

        catalog.close();
    }
}


```



Spark 或者 flink 环境创建表

```sql
CREATE TABLE if not exists test1.test1_iceberg1 USING ICEBERG 
  TBLPROPERTIES(
     'format-version'='2',
     'write.metadata.delete-after-commit.enabled'=true,
      'write.metadata.previous-versions-max'=1,
     'target-file-size-bytes'=268435456
  )
  as select * from test1.test1 limit 0;

```

s3 或者 minio hadoop catalog例子
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      }
    },
    "content": [
      {
        "reader": {
          "name": "rdbmsreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": [
              "*"
            ],
            "connection": [
              {
                "querySql": [
                  "select 1+0 id  ,now() ts1,'test1' as name"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://localhost:3306/ruoyi_vue_camunda?allowPublicKeyRetrieval=true"
                ]
              }
            ],
            "fetchSize": 1024
          }
        },
        "writer": {
          "name": "icebergwriter",
          "parameter": {
            "tableName": "test.test1",
            "writeMode": "truncate",
            "catalogType":"hadoop",
            "warehouse": "s3a://pvc-91d1e2cd-4d25-45c9-8613-6c4f7bf0a4cc/iceberg",
            "hadoopConfig": {

              "fs.s3a.endpoint": "http://localhost:9000",
              "fs.s3a.access.key": "gy0dX5lALP176g6c9fYf",
              "fs.s3a.secret.key": "ReuUrCzzu5wKWAegtswoHIWV389BYl9AB1ZQbiKr",
              "fs.s3a.connection.ssl.enabled": "false",
              "fs.s3a.path.style.access": "true",
              "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
            }
          }
        }
      }
    ]
  }
}
```


hdfs hadoop catalog例子

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      }
    },
    "content": [
      {
        "reader": {
          "name": "rdbmsreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": [
              "*"
            ],
            "connection": [
              {
                "querySql": [
                  "select 1+0 id  ,now() ts1,CAST(1.2 AS DECIMAL(12,2)) dec1,true bool1,'{\"a\":\"1\"}' map1,now() date1,1.3 float1,1.4 double1,'a,b,c' array1,'test1' as name"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://localhost:3306/ruoyi_vue_camunda?allowPublicKeyRetrieval=true"
                ]
              }
            ],
            "fetchSize": 1024
          }
        },
        "writer": {
          "name": "icebergwriter",
          "parameter": {
            "tableName": "test1.test20250219",
            "writeMode": "truncate",
            "catalogType": "hadoop",
            "warehouse": "hdfs://nameservice1/user/hive/iceberg",
            "kerberosKeytabFilePath":"/tmp/hive@XXX.COM.keytab",
            "kerberosPrincipal":"hive@XXX.COM",
            "hadoopConfig": {
              "fs.defaultFS": "hdfs://nameservice1",
              "hadoop.security.authentication": "kerberos",
              "hadoop.kerberos.principal": "hive/_HOST@XXX.COM",
              "hadoop.kerberos.keytab": "/tmp/hive@XXX.COM.keytab",
              "ha.zookeeper.quorum": "nn1:2181,nn2:2181,nn3:2181",
              "dfs.nameservices": "nameservice1",
              "dfs.namenode.rpc-address.nameservice1.namenode371": "nn2:8020",
              "dfs.namenode.rpc-address.nameservice1.namenode265": "nn1:8020",
              "dfs.namenode.keytab.file": "/tmp/hdfs@XXX.COM.keytab",
              "dfs.namenode.keytab.enabled": "true",
              "dfs.namenode.kerberos.principal": "hdfs/_HOST@XXX.COM",
              "dfs.namenode.kerberos.internal.spnego.principal": "HTTP/_HOST@XXX.COM",
              "dfs.ha.namenodes.nameservice1": "namenode265,namenode371",
              "dfs.datanode.keytab.file": "/tmp/hdfs@XXX.COM.keytab",
              "dfs.datanode.keytab.enabled": "true",
              "dfs.datanode.kerberos.principal": "hdfs/_HOST@XXX.COM",
              "dfs.client.use.datanode.hostname": "false",
              "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.balancer.keytab.file": "/tmp/hdfs@XXX.COM.keytab",
              "dfs.balancer.keytab.enabled": "true",
              "dfs.balancer.kerberos.principal": "hdfs/_HOST@XXX.COM"
            }
          }
        }
      }
    ]
  }
}
```


hive catalog例子

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0
      }
    },
    "content": [
      {
        "reader": {
          "name": "rdbmsreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": [
              "*"
            ],
            "connection": [
              {
                "querySql": [
                  "select 1+0 id  ,now() ts1,CAST(1.2 AS DECIMAL(12,2)) dec1,true bool1,'{\"a\":\"1\"}' map1,now() date1,1.3 float1,1.4 double1,'a,b,c' array1,'test1' as name"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://localhost:3306/ruoyi_vue_camunda?allowPublicKeyRetrieval=true"
                ]
              }
            ],
            "fetchSize": 1024
          }
        },
        "writer": {
          "name": "icebergwriter",
          "parameter": {
            "tableName": "test1.test20250219",
            "writeMode": "truncate",
            "catalogType": "hive",
            "uri": "thrift://nn1:9083,thrift://nn2:9083",
            "warehouse": "/warehouse/tablespace/managed/hive",
            "kerberosKeytabFilePath":"/tmp/hive@XXX.COM.keytab",
            "kerberosPrincipal":"hive@XXX.COM",
            "hadoopConfig": {
              "fs.defaultFS": "hdfs://nameservice1",
              "hadoop.security.authentication": "kerberos",
              "hadoop.kerberos.principal": "hive/_HOST@XXX.COM",
              "hadoop.kerberos.keytab": "/tmp/hive@XXX.COM.keytab",
              "ha.zookeeper.quorum": "nn1:2181,nn2:2181,nn3:2181",
              "dfs.nameservices": "nameservice1",
              "dfs.namenode.rpc-address.nameservice1.namenode371": "nn2:8020",
              "dfs.namenode.rpc-address.nameservice1.namenode265": "nn1:8020",
              "dfs.namenode.keytab.file": "/tmp/hdfs@XXX.COM.keytab",
              "dfs.namenode.keytab.enabled": "true",
              "dfs.namenode.kerberos.principal": "hdfs/_HOST@XXX.COM",
              "dfs.namenode.kerberos.internal.spnego.principal": "HTTP/_HOST@XXX.COM",
              "dfs.ha.namenodes.nameservice1": "namenode265,namenode371",
              "dfs.datanode.keytab.file": "/tmp/hdfs@XXX.COM.keytab",
              "dfs.datanode.keytab.enabled": "true",
              "dfs.datanode.kerberos.principal": "hdfs/_HOST@XXX.COM",
              "dfs.client.use.datanode.hostname": "false",
              "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
              "dfs.balancer.keytab.file": "/tmp/hdfs@XXX.COM.keytab",
              "dfs.balancer.keytab.enabled": "true",
              "dfs.balancer.kerberos.principal": "hdfs/_HOST@XXX.COM",
              "hive.metastore.uris":"thrift://nn1:9083,thrift://nn2:9083",
              "hive.server2.authentication":"kerberos",
              "hive.metastore.kerberos.principal":"hive/_HOST@XXX.COM",
              "hive.metastore.sasl.enabled":"true"
            }
          }
        }
      }
    ]
  }
}
```


## 类型转换

| Addax 内部类型                   | Iceberg 数据类型 |
|------------------------------|--------------|
| Integer                      | INTEGER      |
| Long                         | LONG         |
| Double                       | DOUBLE       |
| Float                        | FLOAT        |
| Decimal                      | DECIMAL      |
| String                       | STRING       |
| Boolean                      | BOOLEAN      |
| Date                         | DATE         |
| TIMESTAMP                    | TIMESTAMP    |
| Bytes                        | BINARY       |
| STRING(逗号分隔如'a,b,c')      | ARRAY        |
| STRING(json格式如'{"a":"1"}') | MAP          |

##插件构建

```shell
set JAVA_HOME=E:\jdk\openlogic-openjdk-17.0.13+11-windows-x64
mvn package install -Pdefault -Piceberg   -pl plugin/writer/icebergwriter
```


