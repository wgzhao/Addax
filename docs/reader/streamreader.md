# Stream Reader

Stream Reader 是一个从内存读取数据的插件， 他主要用来快速生成期望的数据并对写入插件进行测试

一个完整的 StreamReader 配置文件如下：

```json
--8<-- "jobs/streamreader.json"
```

上述配置文件将会生成 10条记录（假定channel为1），每条记录的内容如下：

`unique_id,'1989-06-04 08:12:13',1984,1989.64,true,'a long text'`

目前 StreamReader 支持的输出数据类型全部列在上面，分别是：

- `string` 字符类型
- `date` 日期类型
- `long` 所有整型类型
- `double` 所有浮点数
- `bool` 布尔类型
- `bytes` 字节类型

其中 `date` 类型还支持 `dateFormat` 配置，用来指定输入的日期的格式，默认为 `yyyy-MM-dd HH:mm:ss`。比如你的输入可以这样：

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

获得一个 0 至 100 之间的随机浮点数，可以这样配置：

```json
{
  "random": "0,100",
  "type": "double"
}
```

如果要指定浮点数的小数位数，比如指定小数位为2位，则可以这样设定

```json
{
  "random": "0,100,2",
  "type": "double"
}
```

注意： 并不能保证每次生成的小数恰好是2位，如果小数为数为0 ，则小数位数会少于指定的位数。

这里使用 `random` 这个关键字来表示其值为随机值，其值的范围为左右闭区间。

其他类型的随机类型配置如下：

- `long`: random 0, 10 0到10之间的随机数字
- `string`: random 0, 10 0到 10 长度之间的随机字符串
- `bool`: random 0, 10 false 和 true出现的比率
- `double`: random 0, 10 0到10之间的随机浮点数
- `double`: random 0, 10, 2 0到10之间的随机浮点数，小数位为2位  
- `date`: random '2014-07-07 00:00:00', '2016-07-07 00:00:00' 开始时间->结束时间之间的随机时间，日期格式默认(不支持逗号)yyyy-MM-dd HH:mm:ss
- `BYTES`: random 0, 10 0到10长度之间的随机字符串获取其UTF-8编码的二进制串

StreamReader 还支持递增函数，比如我们要得到一个从1开始，每次加5的等差数列，可以这样配置：

```json
{
  "incr": "1,5",
  "type": "long"
}
```

如果需要获得一个递减的数列，则把第二个参数的步长（上例中的5）改为负数即可。步长默认值为1。

递增还支持日期类型( `4.0.1` 版本引入)，比如下面的配置：

```json
{
  "incr": "1989-06-04 09:01:02,2,d",
  "type": "date"
}
```

`incr` 由三部分组成，分别是开始日期，步长以及步长单位，中间用英文逗号(,)分隔。

- 开始日期：正确的日期字符串，默认格式为 `yyyy-MM-dd hh:mm:ss`，如果时间格式不同，则需要配置 `dateFormat` 来指定日期格式，这是必填项
- 步长：每次需要增加的长度，默认为1，如果希望是递减，则填写负数，这是可选项
- 步长单位：按什么时间单位进行递增/递减，默认为按天（day），这是可选项，可选的单位有
  - d/day
  - M/month
  - y/year
  - h/hour
  - m/minute
  - s/second
  - w/week

配置项 `sliceRecordCount` 用来指定要生成的数据条数，如果指定的 `channel`，则实际生成的记录数为 `sliceRecordCount * channel`
