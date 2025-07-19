# HDFS Reader

HDFS Reader provides the ability to read data storage from distributed file system Hadoop HDFS.

Currently HdfsReader supports the following file formats:

- textfile（text）
- orcfile（orc）
- rcfile（rc）
- sequence file（seq）
- Csv(csv)
- parquet

## Features and Limitations

1. Supports textfile, orcfile, parquet, rcfile, sequence file and csv format files, and requires that the file content stores a logically two-dimensional table.
2. Supports reading multiple types of data (represented using String), supports column pruning, supports column constants
3. Supports recursive reading, supports regular expressions (`*` and `?`).
4. Supports common compression algorithms, including GZIP, SNAPPY, ZLIB, etc.
5. Multiple Files can support concurrent reading.
6. Supports sequence file data compression, currently supports lzo compression method.
7. csv type supports compression formats: gzip, bz2, zip, lzo, lzo_deflate, snappy.
8. Currently the Hive version in the plugin is `3.1.1`, Hadoop version is `3.1.1`, writes normally in Hadoop `2.7.x`, Hadoop `3.1.x` and Hive `2.x`, hive `3.1.x` test environments; other versions are theoretically supported, but please test further before using in production environments;
9. Supports `kerberos` authentication

## Configuration Example

```json
--8<-- "jobs/hdfsreader.json"
```

## Configuration Parameters

| Configuration          | Required | Data Type   | Default Value | Description                                                      |
|:-----------------------| :------: |-------------| ------------- |------------------------------------------------------------------|
| path                   | Yes      | string      | None          | File path to read                                                |
| defaultFS              | Yes      | string      | None          | HDFS `NAMENODE` node address, if HA mode is configured, it is the value of `defaultFS` |
| fileType               | Yes      | string      | None          | File type                                                        |
| column                 | Yes      | `list<map>` | None          | List of fields to read                                           |
| fieldDelimiter         | No       | char        | `,`           | Specify text file field delimiter, binary files do not need to specify this |
| encoding               | No       | string      | `utf-8`       | File encoding configuration, currently only supports `utf-8`    |
| nullFormat             | No       | string      | None          | Characters that can represent null, if user configures: `"\\N"`, then if source data is `"\N"`, it's treated as `null` field |
| haveKerberos           | No       | boolean     | None          | Whether to enable Kerberos authentication, if enabled, need to configure the following two items |
| kerberosKeytabFilePath | No       | string      | None          | Kerberos authentication credential file path, e.g. `/your/path/addax.service.keytab` |
| kerberosPrincipal      | No       | string      | None          | Kerberos authentication credential principal, e.g. `addax/node1@WGZHAO.COM` |
| compress               | No       | string      | None          | Specify compression format of files to read                      |
| hadoopConfig           | No       | map         | None          | Can configure some advanced parameters related to Hadoop, such as HA configuration |
| hdfsSitePath           | No       | string      | None          | Path to `hdfs-site.xml`, detailed explanation below             |