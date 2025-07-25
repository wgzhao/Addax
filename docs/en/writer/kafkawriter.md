# Kafka Writer

Kafka Writer plugin implements the functionality of writing data to Kafka in JSON format.

## Example

The following configuration demonstrates how to read data from memory and write to specified topic in Kafka.

### Create Task File

First create a task file `stream2kafka.json` with the following content:

```json
--8<-- "jobs/stream2kafka.json"
```

### Run

Execute `bin/addax.sh stream2kafka.json` command to get output similar to the following:

```shell
--8<-- "output/stream2kafka.txt"
```

We use Kafka's built-in `kafka-console-consumer.sh` to try reading data, output as follows:

```shell
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-1 --from-beginning

{"col8":"41.12,-71.34","col9":"2017-05-25 11:22:33","col6":"hello world","col7":"long text","col4":19890604,"col5":19890604,"col2":"1.1.1.1","col3":1.9890604E7,"col1":916}
```