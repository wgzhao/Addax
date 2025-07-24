# HDFS Writer

HDFS Writer provides the ability to write files in formats like `TextFile`, `ORCFile`, `Parquet` etc. to specified paths in HDFS file system. File content can be associated with tables in Hive.

## Configuration Example

```json
--8<-- "jobs/hdfswriter.json"
```

## Parameters

| Configuration          | Required | Data Type   | Default Value | Description                                                      |
|:-----------------------|:--------:|-------------|---------------|------------------------------------------------------------------|
| path                   | Yes      | string      | None          | File path to read                                                |
| defaultFS              | Yes      | string      | None          | Detailed description below                                       |
| fileType               | Yes      | string      | None          | File type, detailed description below                            |
| fileName               | Yes      | string      | None          | Filename to write, used as prefix                                |
| column                 | Yes      | `list<map>` | None          | List of fields to write                                          |
| writeMode              | Yes      | string      | None          | Write mode, detailed description below                           |
| skipTrash              | No       | boolean     | false         | Whether to skip trash, related to `writeMode` configuration     |
| fieldDelimiter         | No       | string      | `,`           | Field delimiter for text files, not needed for binary files     |
| encoding               | No       | string      | `utf-8`       | File encoding configuration, currently only supports `utf-8`    |
| nullFormat             | No       | string      | None          | Define characters representing null, e.g. if user configures `"\\N"`, then if source data is `"\N"`, treat as `null` field |
| haveKerberos           | No       | boolean     | false         | Whether to enable Kerberos authentication, if enabled, need to configure the following two items |
| kerberosKeytabFilePath | No       | string      | None          | Credential file path for Kerberos authentication, e.g. `/your/path/addax.service.keytab` |
| kerberosPrincipal      | No       | string      | None          | Credential principal for Kerberos authentication, e.g. `addax/node1@WGZHAO.COM` |
| compress               | No       | string      | None          | File compression format, see below                               |
| hadoopConfig           | No       | map         | None          | Can configure some advanced parameters related to Hadoop, such as HA configuration |
| preShell               | No       | `list`      | None          | Shell commands to execute before writing data, e.g. `hive -e "truncate table test.hello"` |