# Kafka Reader

Kafka Reader plugin implements the functionality of reading JSON format messages from Kafka queues. This plugin was introduced in version `4.0.10`.

## Example

The following configuration demonstrates how to read from specified topics in Kafka and output to terminal.

### Create Task File

First create a task file `kafka2stream.json`, with the following content:

```json
--8<-- "jobs/kafka2stream.json"
```

### Run

Execute the `bin/addax.sh kafka2stream.json` command.

## Parameters

| Configuration   | Required | Data Type | Default Value | Description                                                    |
| :-------------- | :------: | --------- | ------------- | -------------------------------------------------------------- |
| brokerList      | Yes      | string    | None          | Broker configuration for connecting to kafka service, like `localhost:9092`, multiple brokers separated by commas (`,`) |
| topic           | Yes      | string    | None          | Topic to write to                                              |
| column          | Yes      | list      | None          | Collection of column names to be synchronized in the configured table, detailed below |
| missingKeyValue | No       | string    | None          | What value to fill when field does not exist, detailed below  |
| properties      | No       | map       | None          | Other kafka connection parameters to be set                    |

### column

`column` is used to specify the keys to read from JSON messages. If set to `*`, it means reading all keys in the message. Note that in this case, the output will not be sorted, meaning the output order of keys for each record is not guaranteed to be consistent.

You can also specify keys to read, for example:

```json
{
  "column": ["col1", "col2", "col3"]
}
```

This way, the plugin will try to read the corresponding keys in the given order. If a key to be read does not exist in a message, the plugin will report an error and exit. If you want it not to exit, you can set `missingKeyValue`, which represents the value to fill when the key to be read does not exist.

Additionally, the plugin will automatically guess the type of the key value being read. If the type cannot be guessed, it will be treated as String type.

## Limitations

1. Only supports Kafka `1.0` and above versions, versions below this cannot be guaranteed to work
2. Currently does not support kafka services with `kerberos` authentication enabled