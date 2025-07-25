# HBase11x SQL Reader

HBase11x SQL Reader plugin implements reading data from [Phoenix(HBase SQL)](https://phoenix.apache.org), supporting HBase version 1.x.

## Configuration Example

Configure a job to synchronize and extract data from Phoenix to local:

```json
{
    "job": {
        "setting": {
            "speed": {
                "byte":-1,
              "channel": 1
            }
        },
        "content": [ {
                "reader": {
                    "name": "hbase11xsqlreader",
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