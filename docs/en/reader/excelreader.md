# Excel Reader

`Excel Reader` plugin implements the ability to read data from Microsoft Excel files.

## Configuration

### Get Sample Files

Download the Excel compressed file for demonstration from [here](../assets/excel_reader_demo.zip) and extract it to the `/tmp/in` directory.
The three folders have the same content, where:

- `demo.xlsx` is the new Excel format
- `demo.xls` is the old Excel format
- `demo_gbk.xlsx` is created under Windows and stored with GBK encoding

File content is shown in the following table:

| No. | Integer Type | Float Type | String Type        | Date Type | Formula Calculation | Cell Formatting |
| --- | ------------ | ---------- | ------------------ | --------- | ------------------- | --------------- |
| 1   | 11           | 1102.234   | Addax 加上中文     | 2021/9/10 | 5544.17             | ¥1,102.23       |
| 2   | 12           | 1103.234   | Addax 加上中文     | 2021/9/11 | 5552.17             | ¥1,103.23       |
| 3   | 13           | 1104.234   | Addax 加上中文     | 2021/9/12 | 5560.17             | ¥1,104.23       |
| 4   | 14           | 1105.234   | Addax 加上中文     | 2021/9/13 | 5568.17             | ¥1,105.23       |
| 5   | 15           | 1106.234   | Addax 加上中文     | 2021/9/14 | 5576.17             | ¥1,106.23       |
| 6   | 16           | 1107.234   | Addax 加上中文     | 2021/9/15 | 5584.17             | ¥1,107.23       |
| 7   | 17           | 1108.234   | Addax 加上中文     | 2021/9/16 | 5592.17             | ¥1,108.23       |
| 8   | 18           | 1109.234   | Addax 加上中文     | 2021/9/17 | 5600.17             | ¥1,109.23       |
| 9   | 19           | 1110.234   | Addax 加上中文     | 2021/9/18 | 5608.17             | ¥1,110.23       |
| 10  | 20           | 1111.234   | Addax 加上中文     | 2021/9/19 | 5616.17             | ¥1,111.23       |
| 11  | 21           | 1112.234   | Addax 加上中文     | 2021/9/20 | 5624.17             | ¥1,112.23       |
| 12  | 22           | 1113.234   | Addax 加上中文     | 2021/9/21 | 5632.17             | ¥1,113.23       |
| 13  | 23           | 1114.234   | Addax 加上中文     | 2021/9/22 | 5640.17             | ¥1,114.23       |
| 14  | 24           | 1115.234   | Addax 加上中文     | 2021/9/23 | 5648.17             | ¥1,115.23       |
| 15  | 25           | 1116.234   | Addax 加上中文     | 2021/9/24 | 5656.17             | ¥1,116.23       |
| 16  | 26           | 1117.234   | Addax 加上中文     | 2021/9/25 | 5664.17             | ¥1,117.23       |
| 17  | 27           | 1118.234   | Addax 加上中文     | 2021/9/26 | 5672.17             | ¥1,118.23       |
| 18  | 28           | 1119.234   | Addax 加上中文     | 2021/9/27 | 5680.17             | ¥1,119.23       |
| 19  | 29           | 1120.234   | Addax 加上中文     | 2021/9/28 | 5688.17             | ¥1,120.23       |
| 20  | 30           | 1121.234   | Addax 加上中文     | 2021/9/29 | 5696.17             | ¥1,121.23       |

The table headers roughly describe the characteristics of cell data.

### Create Job File

Create the following JSON file:

=== "excel2stream.json"

    ```json
    --8<-- "jobs/excelreader.json"
    ```

Save the output content to the `job/excel2stream.json` file and execute the collection command:

```shell
$ bin/addax.sh job/excel2stream.json
```

If there are no errors, you should get the following output:

<details>
<summary>Click to expand</summary>

```shell
--8<-- "output/excelreader.txt"
```

</details>

## Parameters

| Configuration | Required | Type        | Default Value | Description                                      |
| :------------ | -------- | ----------- | ------------- | ------------------------------------------------ |
| path          | Yes      | string/list | None          | Specify the folder to read, multiple can be specified |
| header        | No       | boolean     | false         | Whether the file contains headers                |
| skipRows      | No       | int         | 0             | How many rows to skip at the beginning          |

### header

Whether the Excel file contains headers, if so, skip them.

### skipRows

Specify the number of rows to skip, default is 0, meaning no skipping. Note that if `header` is set to true and `skipRows` is set to 2, it means the first three rows are all skipped.
If `header` is false, it means skipping the first two rows.

### Supported Data Types

The Excel reading functionality implementation depends on the [Apache POI](https://poi.apache.org/) project, which has a very broad definition of cell data types.
It only defines three types: Boolean, Double (numeric), and String. Among them, numeric type includes all integers, decimals, and dates.
Currently, simple distinction is made for numeric types:

1. Use library utility classes to detect if it's a date type, if so, determine it as date type
2. Convert the numeric value to long integer and compare with the original value, if equal, determine as Long type
3. Otherwise determine as Double type

## Limitations

1. Currently only reads the first Sheet of the file and ignores other Sheets
2. Does not support specifying column reading
3. Does not support skipping trailing rows (for example, summary tail rows may not meet requirements)
4. Does not check if the number of columns in each row is equal, Excel must ensure this
5. Only reads files with `xlsx` or `xls` file extensions in the specified directory, other extension files will be ignored with warning messages