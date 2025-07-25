# HBase20 SQL Reader

HBase20 SQL Reader plugin implements reading data from [Phoenix(HBase SQL)](https://phoenix.apache.org), corresponding to HBase2.X and Phoenix5.X versions.

## Configuration Example

Configure a job to synchronize and extract data from Phoenix to local:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hbase20xsqlreader",
          "parameter": {
            "queryServerAddress": "http://127.0.0.1:8765",
            "serialization": "PROTOBUF",
            "table": "TEST",
            "column": ["ID", "NAME"]
          }
        }
      }
    ]
  }
}
```

## Parameters

This plugin is based on [RDBMS Reader](../rdbmsreader), so you can refer to all configuration items of RDBMS Reader.