# StreamReader

StreamReader 是一个从内存读取数据的插件， 他主要用来快速生成期望的数据并对写入插件进行测试

一个完整的 StreamReader 配置文件如下：

```json
{
  "reader": {
    "name": "streamreader",
    "parameter": {
      "column": [
        {
          "value": "unique_id",
          "type": "string"
        },
        {
          "value": "1989-06-04 08:12:13",
          "type": "date",
          "dateFormat": "yyyy-MM-dd HH:mm:ss"
        },
        {
          "value": 1984,
          "type": "long"
        },
        {
          "value": 1989.64,
          "type": "double"
        },
        {
          "value": true,
          "type": "bool"
        },
        {
          "value": "a long text",
          "type": "bytes"
        }
      ],
      "sliceRecordCount": 10
    }
  }
}
```

上述配置文件将会生成 10条记录（假定channel为1），每条记录的内容如下：

`unique_id,'1989-06-04 08:12:13',1984,1989.64,true,'a long text'`

目前 StreamReader 支持的输出数据类型全部列在上面，分别是：

- string 字符类型
- date 日期类型
- long 所有整型类型
- double 所有浮点数
- bool 布尔类型
- bytes 字节类型

其中 date 类型还支持 `dateFormat` 配置，用来指定输入的日期的格式，默认为 `yyyy-MM-dd HH:mm:ss`。比如你的输入可以这样：

```json
{
  "value": "1989/06/04 12:13:14",
  "type": "date",
  "dateFormat": "yyyy/MM/dd HH:mm:ss"
}
```

注意，日期类型不管输入是何种格式，内部都转为 `yyyy-MM-dd HH:mm:ss` 格式。

StreamReader 还支持随机输入功能，比如我们要随机得到0-10之间的任意一个整数，我们可以这样配置列：

```json
{
  "random": "0,10",
  "type": "long"
}
```

这里使用 `random` 这个关键字来表示其值为随机值，其值的范围为左右闭区间。

其他类型的随机类型配置如下：

- `long`: random 0, 10 0到10之间的随机数字
- `string`: random 0, 10 0到10长度之间的随机字符串
- `bool`: random 0, 10 false 和 true出现的比率
- `double`: random 0, 10 0到10之间的随机浮点数
- `date`: random '2014-07-07 00:00:00', '2016-07-07 00:00:00' 开始时间->结束时间之间的随机时间，日期格式默认(不支持逗号)yyyy-MM-dd HH:mm:ss
- `BYTES`: random 0, 10 0到10长度之间的随机字符串获取其UTF-8编码的二进制串

配置项 `sliceRecordCount` 用来指定要生成的数据条数，如果指定的 `channel`，则实际生成的记录数为 `sliceRecordCount * channel`

