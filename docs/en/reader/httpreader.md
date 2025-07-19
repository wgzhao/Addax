# HTTP Reader

HTTP Reader plugin implements the ability to read Restful API data.

## Example

### Sample Interface and Data

The following configuration demonstrates how to get data from a specified API, assuming the accessed interface is:

<http://127.0.0.1:9090/mock/17/LDJSC/ASSET>

The interface accepts GET requests with the following parameters:

| Parameter Name | Example Value |
| -------------- | ------------- |
| CURR_DATE      | 2021-01-17    |
| DEPT           | 9400          |
| USERNAME       | andi          |

The following is a sample of accessed data (actual returned data may vary slightly):

```json
--8<-- "sql/http.json"
```

We need to get partial key value data from the `result` results.

### Configuration

The following configuration implements getting data from the interface and printing to terminal

=== "job/httpreader2stream.json"

```json
--8<-- "jobs/httpreader.json"
```

Save the above content as `job/httpreader2stream.json` file.

### Execution

Execute the following command for collection

```shell
bin/addax.sh job/httpreader2stream.json
```

The output of the above command is roughly as follows:

```
--8<-- "output/httpreader.txt"
```

## Parameters

| Configuration | Required | Data Type | Default Value | Description                                                                          |
| ------------- | :------: | :-------: | :-----------: | ------------------------------------------------------------------------------------ |
| url           | Yes      |  string   |     None      | HTTP address to access                                                               |
| reqParams     | No       |    map    |     None      | Interface request parameters                                                         |
| resultKey     | No       |  string   |     None      | Key value to get results, if getting entire return value, no need to fill          |
| method        | No       |  string   |      get      | Request mode, only supports GET and POST, case insensitive                         |
| column        | Yes      |   list    |     None      | Keys to get, configure as `"*"` to get all key values                              |
| username      | No       |  string   |     None      | Authentication account required for interface request (if any)                      |
| password      | No       |  string   |     None      | Password required for interface request (if any)                                   |
| proxy         | No       |    map    |     None      | Proxy address, see description below                                                |
| headers       | No       |    map    |     None      | Custom request header information                                                    |
| isPage        | No       | boolean   |     None      | Whether interface supports pagination                                                |
| pageParams    | No       |    map    |     None      | Pagination parameters                                                                |

### reqParams

reqParams are request parameters. If the request is `GET` method, it will be appended to the `url` in `k=v` format.
If the request is `POST` mode, `reqParams` will be sent as JSON content in the request body.
In particular, in `POST` mode, if your request body is not a `k-v` structure, you can set the `key` to empty string, like:

```json
{
  "reqParams": {
    "": [123,3456]
  }
}
```

The program will handle this case specially.

### proxy

If the accessed interface needs to go through a proxy, you can configure the `proxy` configuration item, which is a json dictionary containing a required `host` field and an optional `auth` field.

```json
{
  "proxy": {
    "host": "http://127.0.0.1:8080",
    "auth": "user:pass"
  }
}
```

For `socks` proxy (V4, V5), you can write:

```json
{
  "proxy": {
    "host": "socks://127.0.0.1:8080",
    "auth": "user:pass"
  }
}
```

`host` is the proxy address, including proxy type. Currently only supports `http` proxy and `socks` (both V4 and V5) proxy. If the proxy requires authentication, you can configure `auth`, which consists of username and password separated by colon (`:`).

### column

Besides directly specifying keys, `column` also allows using JSON Xpath style to specify key values to get. Suppose you want to read the following JSON file:

```json
{
  "result": [
    {
      "CURR_DATE": "2019-12-09",
      "DEPT": {
        "ID": "9700"
      },
      "KK": [
        {
          "COL1": 1
        },
        {
          "COL2": 2
        }
      ]
    },
    {
      "CURR_DATE": "2021-11-09",
      "DEPT": {
        "ID": "6500"
      },
      "KK": [
        {
          "COL1": 3
        },
        {
          "COL2": 4
        }
      ]
    }
  ]
}
```

If we want to read `CURR_DATE`, `ID`, `COL1`, `COL2` as four fields, your `column` can be configured like this:

```json
{
  "column": [
    "CURR_DATE",
    "DEPT.ID",
    "KK[0].COL1",
    "KK[1].COL2"
  ]
}
```

The execution result is as follows:

```shell
...
2021-10-30 14:01:50.273 [ taskGroup-0] INFO  Channel              - Channel set record_speed_limit to -1, No tps activated.

2019-12-09	9700	1	2
2021-11-09	6500	3	4

2021-10-30 14:01:53.283 [       job-0] INFO  AbstractScheduler    - Scheduler accomplished all tasks.
2021-10-30 14:01:53.284 [       job-0] INFO  JobContainer         - Addax Writer.Job [streamwriter] do post work.
2021-10-30 14:01:53.284 [       job-0] INFO  JobContainer         - Addax Reader.Job [httpreader] do post work.
2021-10-30 14:01:53.286 [       job-0] INFO  JobContainer         - PerfTrace not enable!
2021-10-30 14:01:53.289 [       job-0] INFO  JobContainer         -
Task start time                    : 2021-10-30 14:01:50
Task end time                      : 2021-10-30 14:01:53
Task total duration                :                  3s
Task average throughput            :               10B/s
Record write speed                 :              0rec/s
Total records read                 :                   2
Total read/write failures          :                   0
```

Note: If you specify a non-existent key, it returns NULL value directly.

### isPage

The `isPage` parameter is used to specify whether the interface supports pagination. It is a boolean value. If `true`, it means the interface supports pagination, otherwise it doesn't.

When the interface supports pagination, it will automatically paginate reading until the number of records returned by the interface's last return is less than the number of records per page.

### pageParams

The `pageParams` parameter only takes effect when the `isPage` parameter is `true`. It is a JSON dictionary containing two optional fields `pageIndex` and `pageSize`.

`pageIndex` is used to indicate the current page for pagination. It is a JSON field containing two optional fields `key` and `value`, where `key` specifies the parameter name for page number, and `value` specifies the current page number value.

`pageSize` is used to indicate the page size for pagination. It is a JSON field containing two optional fields `key` and `value`, where `key` specifies the parameter name for page size, and `value` specifies the page size value.

The default values for these two parameters are:

```json
{
  "pageParams": {
    "pageIndex": {
      "key": "pageIndex",
      "value": 1
    },
    "pageSize": {
      "key": "pageSize",
      "value": 100
    }
  }
}
```

If your interface pagination parameters are not `pageIndex` and `pageSize`, you can specify them through the `pageParams` parameter. For example:

```json
{
  "isPage": true,
  "pageParams": {
    "pageIndex": {
      "key": "page",
      "value": 1
    },
    "pageSize": {
      "key": "size",
      "value": 100
    }
  }
}
```

This means the pagination parameters passed to the interface are `page=1&size=100`.

## Limitations

1. The returned result must be JSON type
2. Currently all key values are treated as string type
3. Interface Token authentication mode not yet supported
4. Pagination retrieval not yet supported
5. Proxy only supports `http` mode