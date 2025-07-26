# HBase20 Reader

HBase20 Reader plugin supports reading data from HBase 2.x version. Its implementation method is to connect to remote HBase service through HBase's Java client and read data within the `rowkey` range you specify through Scan method.

## Configuration

The following demonstration is based on the table and data created below:

```shell
create 'users', {NAME=>'address', VERSIONS=>100},{NAME=>'info',VERSIONS=>1000}
put 'users', 'lisi', 'address:country', 'china1', 20200101
put 'users', 'lisi', 'address:province',    'beijing1', 20200101
put 'users', 'lisi', 'info:age',        27, 20200101
put 'users', 'lisi', 'info:birthday',   '1987-06-17', 20200101
put 'users', 'lisi', 'info:company',    'baidu1', 20200101
put 'users', 'xiaoming', 'address:city',    'hangzhou1', 20200101
put 'users', 'xiaoming', 'address:country', 'china1', 20200101
put 'users', 'xiaoming', 'address:province',    'zhejiang1',20200101
put 'users', 'xiaoming', 'info:age',        29, 20200101
put 'users', 'xiaoming', 'info:birthday',   '1987-06-17',20200101
```

For detailed configuration and parameters, please refer to the original HBase20 Reader documentation.