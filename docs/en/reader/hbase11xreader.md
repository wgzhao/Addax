# HBase11X Reader

HBase11X Reader plugin supports reading data from HBase 1.x version. Its implementation method is to connect to remote HBase service through HBase's Java client and read data within the `rowkey` range you specify through Scan method.

## Configuration

### Table Creation and Data Population

The following demonstration is based on the table and data created below:

```shell
create 'users', 'address','info'
put 'users', 'lisi', 'address:country', 'china'
put 'users', 'lisi', 'address:province',    'beijing'
put 'users', 'lisi', 'info:age',        27
put 'users', 'lisi', 'info:birthday',   '1987-06-17'
put 'users', 'lisi', 'info:company',    'baidu'
put 'users', 'xiaoming', 'address:city',    'hangzhou'
put 'users', 'xiaoming', 'address:country', 'china'
put 'users', 'xiaoming', 'address:province',    'zhejiang'
put 'users', 'xiaoming', 'info:age',        29
put 'users', 'xiaoming', 'info:birthday',   '1987-06-17'
put 'users', 'xiaoming', 'info:company',    'alibaba'
```

#### normal Mode

Read HBase table as a normal two-dimensional table (horizontal table), reading the latest version data. For example:

```shell
hbase(main):017:0> scan 'users'
ROW           COLUMN+CELL
 lisi         column=address:city, timestamp=1457101972764, value=beijing
 lisi         column=address:country, timestamp=1457102773908, value=china
 lisi         column=address:province, timestamp=1457101972736, value=beijing
 lisi         column=info:age, timestamp=1457101972548, value=27
 lisi         column=info:birthday, timestamp=1457101972604, value=1987-06-17
 lisi         column=info:company, timestamp=1457101972653, value=baidu
 xiaoming     column=address:city, timestamp=1457082196082, value=hangzhou
 xiaoming     column=address:country, timestamp=1457082195729, value=china
```

For detailed configuration and parameters, please refer to the original HBase11X Reader documentation.