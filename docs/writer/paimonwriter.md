# Paimon Writer

Paimon Writer 提供向 已有的paimon表写入数据的能力。

## 配置样例

```json
--8<-- "jobs/paimonwriter.json"
```

## 参数说明

| 配置项          | 是否必须 | 数据类型   | 默认值 | 说明                                             |
|:-------------|:----:|--------|----|------------------------------------------------|
| dbName       |  是   | string | 无  | 要写入的paimon数据库名                                 |
| tableName    |  是   | string | 无  | 要写入的paimon表名                                   |
| writeMode    |  是   | string | 无  | 写入模式，详述见下                                      |
| paimonConfig |  是   | json   | {} | 里可以配置与 Paimon catalog和Hadoop 相关的一些高级参数，比如HA的配置 |



### writeMode

写入前数据清理处理模式：

- append，写入前不做任何处理，直接写入，不清除原来的数据。
- truncate 写入前先清空表，再写入。

### paimonConfig

`paimonConfig` 里可以配置与 Paimon catalog和Hadoop 相关的一些高级参数，比如HA的配置

本地目录创建paimon表

pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.test</groupId>
    <artifactId>paimon-java-api-test</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <hadoop.version>3.2.4</hadoop.version>
        <woodstox.version>7.0.0</woodstox.version>
    </properties>
<dependencies>
    <dependency>
        <groupId>org.apache.paimon</groupId>
        <artifactId>paimon-bundle</artifactId>
        <version>1.0.0</version>
    </dependency>


    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-common</artifactId>
        <version>${hadoop.version}</version>
        <exclusions>
            <exclusion>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.codehaus.jackson</groupId>
                <artifactId>jackson-core-asl</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.codehaus.jackson</groupId>
                <artifactId>jackson-mapper-asl</artifactId>
            </exclusion>
            <exclusion>
                <groupId>com.fasterxml.woodstox</groupId>
                <artifactId>woodstox-core</artifactId>
            </exclusion>
            <exclusion>
                <groupId>commons-codec</groupId>
                <artifactId>commons-codec</artifactId>
            </exclusion>
            <exclusion>
                <groupId>commons-net</groupId>
                <artifactId>commons-net</artifactId>
            </exclusion>
            <exclusion>
                <groupId>io.netty</groupId>
                <artifactId>netty</artifactId>
            </exclusion>
            <exclusion>
                <groupId>log4j</groupId>
                <artifactId>log4j</artifactId>
            </exclusion>
            <exclusion>
                <groupId>net.minidev</groupId>
                <artifactId>json-smart</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.codehaus.jettison</groupId>
                <artifactId>jettison</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.eclipse.jetty</groupId>
                <artifactId>jetty-server</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.xerial.snappy</groupId>
                <artifactId>snappy-java</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.apache.zookeeper</groupId>
                <artifactId>zookeeper</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.eclipse.jetty</groupId>
                <artifactId>jetty-util</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-aws</artifactId>
        <version>${hadoop.version}</version>
        <exclusions>
            <exclusion>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.codehaus.jackson</groupId>
                <artifactId>jackson-core-asl</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.codehaus.jackson</groupId>
                <artifactId>jackson-mapper-asl</artifactId>
            </exclusion>
            <exclusion>
                <groupId>com.fasterxml.woodstox</groupId>
                <artifactId>woodstox-core</artifactId>
            </exclusion>
            <exclusion>
                <groupId>commons-codec</groupId>
                <artifactId>commons-codec</artifactId>
            </exclusion>
            <exclusion>
                <groupId>commons-net</groupId>
                <artifactId>commons-net</artifactId>
            </exclusion>
            <exclusion>
                <groupId>io.netty</groupId>
                <artifactId>netty</artifactId>
            </exclusion>
            <exclusion>
                <groupId>log4j</groupId>
                <artifactId>log4j</artifactId>
            </exclusion>
            <exclusion>
                <groupId>net.minidev</groupId>
                <artifactId>json-smart</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.codehaus.jettison</groupId>
                <artifactId>jettison</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.eclipse.jetty</groupId>
                <artifactId>jetty-server</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.xerial.snappy</groupId>
                <artifactId>snappy-java</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.apache.zookeeper</groupId>
                <artifactId>zookeeper</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.eclipse.jetty</groupId>
                <artifactId>jetty-util</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-mapreduce-client-core</artifactId>
        <version>${hadoop.version}</version>
        <exclusions>
            <exclusion>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
            </exclusion>
            <exclusion>
                <groupId>commons-codec</groupId>
                <artifactId>commons-codec</artifactId>
            </exclusion>
            <exclusion>
                <groupId>io.netty</groupId>
                <artifactId>netty</artifactId>
            </exclusion>
            <exclusion>
                <groupId>org.eclipse.jetty</groupId>
                <artifactId>jetty-util</artifactId>
            </exclusion>
        </exclusions>
    </dependency>


    <dependency>
        <groupId>com.fasterxml.woodstox</groupId>
        <artifactId>woodstox-core</artifactId>
        <version>${woodstox.version}</version>
    </dependency>
</dependencies>
</project>
```

```java

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class CreatePaimonTable {

    public static Catalog createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("file:///g:/paimon"));
        return CatalogFactory.createCatalog(context);
    }

    public static void main(String[] args) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("id");
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("name", DataTypes.STRING());
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");//由于paimon java api 限制需要bucket>0
        options.put("bucket-key", "id");
        options.put("file.format", "orc");
        options.put("file.compression", "lz4");
        options.put("lookup.cache-spill-compression", "lz4");
        options.put("spill-compression", "LZ4");
        options.put("orc.compress", "lz4");
        options.put("manifest.format", "orc");

        schemaBuilder.options(options);
        Schema schema = schemaBuilder.build();

        Identifier identifier = Identifier.create("test", "test2");
        try {
            Catalog catalog = CreatePaimonTable.createFilesystemCatalog();
            catalog.createTable(identifier, schema, true);
        } catch (Catalog.TableAlreadyExistException e) {
            e.printStackTrace();
        } catch (Catalog.DatabaseNotExistException e) {
            e.printStackTrace();
        }


    }
}


```

Spark 或者 flink 环境创建表

```sql
CREATE TABLE if not exists test.test2  tblproperties (
    'primary-key' = 'id',
    'bucket' = '1',
    'bucket-key' = 'id'
    'file.format'='orc',
    'file.compression'='lz4',
    'lookup.cache-spill-compression'='lz4',
    'spill-compression'='LZ4',
    'orc.compress'='lz4',
    'manifest.format'='orc'
)

```


```json
{
					"name": "paimonwriter",
					"parameter": {
						"dbName": "test",
                        "tableName": "test2",
                        "writeMode": "truncate",
                        "paimonConfig": {
                           "warehouse": "file:///g:/paimon",
                           "metastore": "filesystem"
                         }
					}
}
```
```json
{
  "paimonConfig": {
    "warehouse": "hdfs://nameservice1/user/hive/paimon",
    "metastore": "filesystem",
    "fs.defaultFS":"hdfs://nameservice1",
    "hadoop.security.authentication" : "kerberos",
    "hadoop.kerberos.principal" : "hive/_HOST@XXXX.COM",
    "hadoop.kerberos.keytab" : "/tmp/hive@XXXX.COM.keytab",
    "ha.zookeeper.quorum" : "test-pr-nn1:2181,test-pr-nn2:2181,test-pr-nn3:2181",
    "dfs.nameservices" : "nameservice1",
    "dfs.namenode.rpc-address.nameservice1.namenode371" : "test-pr-nn2:8020",
    "dfs.namenode.rpc-address.nameservice1.namenode265": "test-pr-nn1:8020",
    "dfs.namenode.keytab.file" : "/tmp/hdfs@XXXX.COM.keytab",
    "dfs.namenode.keytab.enabled" : "true",
    "dfs.namenode.kerberos.principal" : "hdfs/_HOST@XXXX.COM",
    "dfs.namenode.kerberos.internal.spnego.principal" : "HTTP/_HOST@XXXX.COM",
    "dfs.ha.namenodes.nameservice1" : "namenode265,namenode371",
    "dfs.datanode.keytab.file" : "/tmp/hdfs@XXXX.COM.keytab",
    "dfs.datanode.keytab.enabled" : "true",
    "dfs.datanode.kerberos.principal" : "hdfs/_HOST@XXXX.COM",
    "dfs.client.use.datanode.hostname" : "false",
    "dfs.client.failover.proxy.provider.nameservice1" : "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "dfs.balancer.keytab.file" : "/tmp/hdfs@XXXX.COM.keytab",
    "dfs.balancer.keytab.enabled" : "true",
    "dfs.balancer.kerberos.principal" : "hdfs/_HOST@XXXX.COM"
  }
}
```


## 类型转换

| Addax 内部类型 | Paimon 数据类型                  |
|------------|------------------------------|
| Integer    | TINYINT,SMALLINT,INT,INTEGER |
| Long       | BIGINT                       |
| Double     | FLOAT,DOUBLE,DECIMAL         |
| String     | STRING,VARCHAR,CHAR          |
| Boolean    | BOOLEAN                      |
| Date       | DATE,TIMESTAMP               |
| Bytes      | BINARY                       |

