# Stream Reader

Stream Reader is a plugin that reads data from memory, mainly used to quickly generate expected data and test write plugins.

A complete StreamReader configuration file is as follows:

```json
--8<-- "jobs/streamreader.json"
```

The above configuration file will generate 10 records (assuming channel is 1), with each record containing:

`unique_id,'1989-06-04 08:12:13',1984,1989.64,true,'a long text'`

Currently StreamReader supports all output data types listed above:

- `string` String type
- `date` Date type  
- `long` All integer types
- `double` All floating point numbers
- `bool` Boolean type
- `bytes` Byte type

The `date` type also supports `dateFormat` configuration to specify the format of input dates, default is `yyyy-MM-dd HH:mm:ss`. For example, your input can be like this:

```json
{
  "value": "1989/06/04 12:13:14",
  "type": "date",
  "dateFormat": "yyyy/MM/dd HH:mm:ss"
}
```

Note that regardless of the input format for date type, it is internally converted to `yyyy-MM-dd HH:mm:ss` format.

StreamReader also supports random input functionality. For example, to randomly get any integer between 0-10, we can configure the column like this:

```json
{
  "random": "0,10",
  "type": "long"
}
```

To get a random floating point number between 0 and 100, configure like this:

```json
{
  "random": "0,100",
  "type": "double"
}
```

To specify decimal places for floating point numbers, e.g., 2 decimal places, configure like this:

```json
{
  "random": "0,100,2",
  "type": "double"
}
```

Note: It cannot guarantee that the generated decimal always has exactly 2 places. If the decimal part is 0, the decimal places will be fewer than specified.

Here we use the `random` keyword to indicate its value is random, with the range being a closed interval.

Other random type configurations are as follows:

- `long`: random 0, 10 - random number between 0 and 10
- `string`: random 0, 10 - random string of length between 0 and 10
- `bool`: random 0, 10 - ratio of false and true occurrences
- `double`: random 0, 10 - random floating point between 0 and 10
- `double`: random 0, 10, 2 - random floating point between 0 and 10 with 2 decimal places
- `date`: random '2014-07-07 00:00:00', '2016-07-07 00:00:00' - random time between start time and end time, default date format (commas not supported) yyyy-MM-dd HH:mm:ss
- `BYTES`: random 0, 10 - random string of length between 0 and 10, get its UTF-8 encoded binary string

StreamReader also supports increment functions. For example, to get an arithmetic sequence starting from 1 with increment of 5, configure like this:

```json
{
  "incr": "1,5",
  "type": "long"
}
```

To get a decreasing sequence, change the step size (5 in the above example) to negative. Default step size is 1.

Increment also supports date type (introduced in version `4.0.1`), for example:

```json
{
  "incr": "1989-06-04 09:01:02,2,d",
  "type": "date"
}
```

`incr` consists of three parts: start date, step size, and step unit, separated by English commas (,).

- Start date: Correct date string, default format is `yyyy-MM-dd hh:mm:ss`. If time format is different, need to configure `dateFormat` to specify date format. This is mandatory.
- Step size: Length to increase each time, default is 1. For decreasing, fill in negative number. This is optional.
- Step unit: What time unit to increment/decrement by, default is by day. This is optional. Available units:
  - d/day
  - M/month
  - y/year
  - h/hour
  - m/minute
  - s/second
  - w/week

Configuration item `sliceRecordCount` specifies the number of data records to generate. If `channel` is specified, actual generated records = `sliceRecordCount * channel`