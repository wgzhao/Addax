# Excel Writer

`excelwriter` 实现了将数据写入到 Excel 文件的功能

## 配置示例

我们假定从内存读取数据，并写入到 Excel 文件中

```json
--8<-- "jobs/excelwriter.json"
```

讲上述内容保存为 `job/stream2excel.json`

执行下面的命令：

```shell
bin/addax.sh job/stream2excel.sh
```

应该得到类似如下的输出

<details>
<summary>点击展开</summary>

```shell
--8<-- "output/excelwriter.txt"
```
</details>

## 参数说明

| 配置项   | 是否必须 | 类型   | 默认值 | 描述                                                 |
| :------- | -------- | ------ | ------ | ---------------------------------------------------- |
| path     | 是       | string | 无     | 指定文件保存的目录, 指定的目录如果不存在，则尝试创建 |
| fileName | 是       | string | 无     | 要生成的excel 文件名，详述如下                       |
| header   | 否       | list   | 无     | Excel 表头                                           |

### fileName

如果配置的 `fileName` 没有后缀，则自动加上 `.xlsx`；
如果后缀为 `.xls`，则报错，因为当前仅生成 Excel 97 以后的文件格式，即 `.xlsx` 后缀的文件

### header

如果不指定 `header` ，则生成的 Excel 文件没有表头，只有数据。
注意，插件不关心 header 的数量是否匹配数据中的列数，也就是说表头的列数并不要求和接下来的数据的列数相等。

## 限制

1. 当前仅生成一个 Excel 文件，且没有考虑行数和列数是否超过了 Excel 的限定
2. 如果指定的目录下有同名文件，当前会被覆盖，后续会统一处理目标目录的问题
3. 当前日期格式的数据，设置单元格样式为 `yyyy-MM-dd HH:mm:ss`，且不能定制
4. 不支持二进制类型的数据写入
