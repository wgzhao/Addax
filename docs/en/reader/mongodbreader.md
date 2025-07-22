# MongoDB Reader

MongoDBReader plugin uses MongoDB's Java client MongoClient to perform MongoDB read operations.

## Configuration Example

This example reads a table from MongoDB and prints to terminal

=== "job/mongo2stream.json"

  ```json
  --8<-- "jobs/mongoreader.json"
  ```

## Parameters

| Configuration | Required | Type   | Default Value | Description                                               |
| :------------ | :------: | ------ | ------------- | --------------------------------------------------------- |
| address       | Yes      | list   | None          | MongoDB data address information, multiple can be written |
| username      | No       | string | None          | MongoDB username                                          |
| password      | No       | string | None          | MongoDB password                                          |
| database      | Yes      | string | None          | MongoDB database                                          |
| collection    | Yes      | string | None          | MongoDB collection name                                   |
| column        | Yes      | list   | None          | MongoDB document column names, does not support `["*"]` to get all columns |
| query         | No       | string | None          | Custom query conditions                                   |
| fetchSize     | No       | int    | 2048          | Batch size for retrieving records                         |

### collection

The `collection` here currently only supports a single collection, so the type is set to string rather than the array type common in other plugins. This is particularly noteworthy.

### column

`column` is used to specify the field names to be read. Here we make two assumptions about field name composition:

- Cannot start with single quote (`'`)
- Cannot consist entirely of numbers and dots (`.`)

Based on the above assumptions, we can simplify the `column` configuration while also specifying some constants as supplementary fields. For example, when collecting a table, we generally need to add collection time, collection source and other constants, which can be configured like this:

```json
{
  "column": [
    "col1",
    "col2",
    "col3",
    "'source_mongodb'",
    "20211026",
    "123.12"
  ]
}
```

The last three fields in the above configuration are constants, treated as string type, integer type, and floating point type respectively.

### query

`query` is a BSON string that conforms to MongoDB query format, for example:

```json
{
  "query": "{amount: {$gt: 140900}, oc_date: {$gt: 20190110}}"
}
```

The above query is similar to `where amount > 140900 and oc_date > 20190110` in SQL.

## Type Conversion

| Addax Internal Type | MongoDB Data Type |
| ------------------- | ----------------- |
| Long                | int, Long         |
| Double              | double            |
| String              | string, array     |
| Date                | date              |
| Boolean             | boolean           |
| Bytes               | bytes             |