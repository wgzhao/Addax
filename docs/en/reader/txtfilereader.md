# Text File Reader

The TxtFileReader plugin reads data from text files with configurable delimiters and encodings.

## Example

Sample CSV file (`/tmp/users.csv`):

```csv
id,name,age,email
1,John Doe,30,john@example.com
2,Jane Smith,25,jane@example.com
3,Bob Johnson,35,bob@example.com
```

Configuration to read the CSV file:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/tmp/users.csv",
            "encoding": "UTF-8",
            "column": [
              {
                "index": 0,
                "type": "long"
              },
              {
                "index": 1,
                "type": "string"
              },
              {
                "index": 2,
                "type": "long"
              },
              {
                "index": 3,
                "type": "string"
              }
            ],
            "fieldDelimiter": ",",
            "skipHeader": true
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

## Parameters

### Required Parameters

| Parameter | Description | Type | Default |
|-----------|-------------|------|---------|
| path | File path or directory path | string | None |
| column | Column configuration | array | None |

### Optional Parameters

| Parameter | Description | Type | Default |
|-----------|-------------|------|---------|
| encoding | File encoding | string | UTF-8 |
| fieldDelimiter | Field delimiter | string | , |
| compress | Compression format | string | None |
| skipHeader | Skip first line | boolean | false |
| nullFormat | Null value representation | string | \\N |

## Column Configuration

Each column in the `column` array can be configured with:

| Property | Description | Type | Required |
|----------|-------------|------|----------|
| index | Column index (0-based) | integer | Yes |
| type | Data type | string | Yes |
| value | Constant value | string | No |

### Supported Data Types

- `long`: Integer numbers
- `double`: Floating point numbers  
- `string`: Text data
- `date`: Date and time
- `bool`: Boolean values

## Path Configuration

### Single File

```json
{
  "path": "/data/users.csv"
}
```

### Multiple Files with Wildcards

```json
{
  "path": "/data/users_*.csv"
}
```

### Directory

```json
{
  "path": "/data/csv_files/"
}
```

## Delimiter Examples

### Tab-separated Values

```json
{
  "fieldDelimiter": "\t"
}
```

### Pipe-separated Values

```json
{
  "fieldDelimiter": "|"
}
```

### Fixed-width (using spaces)

```json
{
  "fieldDelimiter": " "
}
```

## Compression Support

### GZIP Files

```json
{
  "path": "/data/users.csv.gz",
  "compress": "gzip"
}
```

### BZIP2 Files

```json
{
  "path": "/data/users.csv.bz2",  
  "compress": "bzip2"
}
```

## Advanced Examples

### Reading JSON Lines Format

```json
{
  "reader": {
    "name": "txtfilereader",
    "parameter": {
      "path": "/data/users.jsonl",
      "encoding": "UTF-8",
      "column": [
        {
          "index": 0,
          "type": "string"
        }
      ],
      "fieldDelimiter": "\n"
    }
  }
}
```

### Reading Log Files

```json
{
  "reader": {
    "name": "txtfilereader", 
    "parameter": {
      "path": "/var/log/app.log",
      "encoding": "UTF-8",
      "column": [
        {
          "index": 0,
          "type": "string"
        }
      ],
      "fieldDelimiter": "\n",
      "nullFormat": ""
    }
  }
}
```

### Constant Values

```json
{
  "column": [
    {
      "index": 0,
      "type": "long"
    },
    {
      "index": 1, 
      "type": "string"
    },
    {
      "type": "string",
      "value": "batch_001"
    }
  ]
}
```

## Error Handling

### Invalid Data Types

When a field cannot be converted to the specified type, it will be treated as dirty data according to your error handling configuration.

### Missing Files

If specified files don't exist, the job will fail. Use wildcards carefully to ensure at least one file matches.

### Encoding Issues

If file encoding doesn't match the specified encoding, character corruption may occur. Always verify the actual file encoding.

## Performance Tips

### Large Files

For very large files, consider splitting them or using multiple channels:

```json
{
  "setting": {
    "speed": {
      "channel": 4
    }
  }
}
```

### Memory Usage

The reader loads data in chunks, so memory usage is generally stable regardless of file size.

### Network Files

When reading from network-mounted drives, network latency may affect performance. Consider copying files locally first for better performance.