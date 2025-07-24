# ElasticSearchReader

ElasticSearchReader plugin implements the functionality of reading indexes from [Elasticsearch](https://www.elastic.co/cn/elasticsearch/). It uses the Rest API provided by Elasticsearch (default port 9200) to execute specified query statements and batch retrieve data.

## Example

Assume the index content to be retrieved is as follows

```json
--8<-- "sql/es.json"
```

Configure a task to read data from Elasticsearch and print to terminal

=== "job/es2stream.json"

  ```json
  --8<-- "jobs/esreader.json"
  ```

Save the above content as `job/es2stream.json`

Execute the following command for collection

```shell
bin/addax.sh job/es2stream.json
```

The output result is similar to the following (output records are reduced):

```
--8<-- "output/esreader.txt"
```

## Parameters

| Configuration | Required | Type    | Default Value          | Description                                                      |
| :------------ | :------: | ------- | ---------------------- | ---------------------------------------------------------------- |
| endpoint      | Yes      | string  | None                   | ElasticSearch connection address                                 |
| accessId      | No       | string  | `""`                   | User in http auth                                                |
| accessKey     | No       | string  | `""`                   | Password in http auth                                            |
| index         | Yes      | string  | None                   | Index name in elasticsearch                                      |
| type          | No       | string  | index name             | Type name of index in elasticsearch                              |
| search        | Yes      | list    | `[]`                   | JSON format API search data body                                 |
| column        | Yes      | list    | None                   | Fields to be read                                                |
| timeout       | No       | int     | 60                     | Client timeout (unit: seconds)                                  |
| discovery     | No       | boolean | false                  | Enable node discovery (polling) and periodically update server list in client |
| compression   | No       | boolean | true                   | HTTP request, enable compression                                 |
| multiThread   | No       | boolean | true                   | HTTP request, whether multi-threaded                            |
| searchType    | No       | string  | `dfs_query_then_fetch` | Search type                                                      |
| headers       | No       | map     | `{}`                   | HTTP request headers                                             |
| scroll        | No       | string  | `""`                   | Scroll pagination configuration                                  |

### search

The search configuration item allows configuration of content that meets Elasticsearch API query requirements, like this:

```json
{
  "query": {
    "match": {
      "message": "myProduct"
    }
  },
  "aggregations": {
    "top_10_states": {
      "terms": {
        "field": "state",
        "size": 10
      }
    }
  }
}
```

### searchType

searchType currently supports the following types:

- dfs_query_then_fetch
- query_then_fetch
- count
- scan