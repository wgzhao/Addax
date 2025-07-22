# Stream Writer

Stream Writer is a plugin that writes data to memory, generally used to write acquired data to terminal for debugging data processing of read plugins.

A typical Stream Writer configuration is as follows:

```json
{
  "name": "streamwriter",
  "parameter": {
    "encoding": "UTF-8",
    "print": true,
    "nullFormat": "NULL"
  }
}
```

The above configuration will print the acquired data directly to terminal. Where `nullFormat` is used to specify how to represent null values in terminal, default is string `NULL`. If you don't want to print null values, you can set it to `""`.

This plugin also supports writing data to files, configured as follows:

```json
{
  "name": "streamwriter",
  "parameter": {
    "encoding": "UTF-8",
    "path": "/tmp/out",
    "fileName": "out.txt",
    "fieldDelimiter": ",",
    "recordNumBeforeSleep": "100",
    "sleepTime": "5"
  }
}
```

In the above configuration:

- `fieldDelimiter` represents field delimiter, default is tab character (`\t`)
- `recordNumBeforeSleep` represents how many records to acquire before executing sleep, default is 0, meaning this feature is disabled
- `sleepTime` represents how long to sleep, in seconds, default is 0, meaning this feature is disabled

The meaning of the above configuration is to write data to `/tmp/out/out.txt` file, sleep for 5 seconds after acquiring every 100 records.