# ElasticSearch Writer

ElasticSearch Writer plugin is used to write data to [ElasticSearch](https://www.elastic.co/cn/elastic-stack/).
It is implemented through elasticsearch's rest api interface, writing data to elasticsearch in batches.

## Configuration Example

=== "job/stream2es.json"

```json
--8<-- "jobs/eswriter.json"
```

## Parameters

| Configuration    | Required | Data Type   | Default Value | Description                                                           |
| :--------------- | :------: | ----------- | ------------- | --------------------------------------------------------------------- |
| endpoint         | Yes      | string      | None          | ElasticSearch connection address, if cluster, multiple addresses separated by comma (,) |
| accessId         | No       | string      | Empty         | User in http auth, default is empty                                  |
| accessKey        | No       | string      | Empty         | Password in http auth                                                 |
| index            | Yes      | string      | None          | Index name                                                            |
| type             | No       | string      | `default`     | Index type                                                            |
| cleanup          | No       | boolean     | false         | Whether to delete original table                                      |
| batchSize        | No       | int         | 1000          | Number of records in each batch                                       |
| trySize          | No       | int         | 30            | Number of retries after failure                                       |
| timeout          | No       | int         | 600000        | Client timeout in milliseconds (ms)                                  |
| discovery        | No       | boolean     | false         | Enable node discovery (polling) and periodically update server list in client |
| compression      | No       | boolean     | true          | Whether to enable http request compression                            |
| multiThread      | No       | boolean     | true          | Whether to enable multi-threaded http requests                       |
| ignoreWriteError | No       | boolean     | false         | Whether to retry on write error, if `true` means always retry, otherwise ignore the record |
| ignoreParseError | No       | boolean     | true          | Whether to continue writing when data format parsing error occurs    |
| alias            | No       | string      | None          | Alias to write after data import is completed                        |
| aliasMode        | No       | string      | append        | Mode for adding alias after data import completion, append (add mode), exclusive (keep only this one) |
| settings         | No       | map         | None          | Settings when creating index, same as elasticsearch official         |
| splitter         | No       | string      | `,`           | If inserted data is array, use specified delimiter                    |
| column           | Yes      | `list<map>` | None          | Field types, the example in the document includes all supported field types |
| dynamic          | No       | boolean     | false         | Don't use addax mappings, use es's own automatic mappings            |

## Constraints

- If importing id, data import failures will also retry, re-import will only overwrite, ensuring data consistency
- If not importing id, it's append_only mode, elasticsearch automatically generates id, speed will improve about 20%, but data cannot be repaired, suitable for log-type data (low precision requirements)