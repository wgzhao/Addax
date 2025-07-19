# Excel Writer

Excel Writer implements the functionality of writing data to Excel files.

## Configuration Example

We assume reading data from memory and writing to Excel file:

```json
--8<-- "jobs/excelwriter.json"
```

Save the above content as `job/stream2excel.json`

Execute the following command:

```shell
bin/addax.sh job/stream2excel.sh
```

You should get output similar to the following:

<details>
<summary>Click to expand</summary>

```shell
--8<-- "output/excelwriter.txt"
```
</details>

## Parameters

| Configuration | Required | Type   | Default Value | Description                                                     |
| :------------ | -------- | ------ | ------------- | --------------------------------------------------------------- |
| path          | Yes      | string | None          | Specify the directory to save files, create if directory doesn't exist |
| fileName      | Yes      | string | None          | Excel filename to generate, detailed description below         |
| header        | No       | list   | None          | Excel header                                                    |

### fileName

For detailed fileName configuration, please refer to the original Excel Writer documentation.