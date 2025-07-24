# Redis Reader

Redis Reader plugin is used to read [Redis RDB](https://redis.io) data.

## Configuration Example

```json
--8<-- "jobs/redisreader.json"
```

## Parameters

| Configuration | Required | Default Value | Description                                                                        |
| :------------ | :------: | ------------- | ---------------------------------------------------------------------------------- |
| uri           | Yes      | No            | Redis connection, supports multiple local rdb files/network rdb files, if cluster, fill in all master node addresses |
| db            | No       | None          | Database index to read, if not filled, read all databases                         |
| include       | No       | None          | Keys to include, supports regular expressions                                      |
| exclude       | No       | None          | Keys to exclude, supports regular expressions                                      |

## Constraints

1. Does not support directly reading any redis server that does not support the `sync` command. If needed, please read from backed up rdb files.
2. If it's a native redis cluster, please fill in all master node TCP addresses. The `redisreader` plugin will automatically dump rdb files from all nodes.
3. Only parses `String` data type, other composite types (`Sets`, `List`, etc. will be ignored)