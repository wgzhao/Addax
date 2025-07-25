# Quick Start

## Download and Install

### Download

You can download the installation package from the [release page](https://github.com/wgzhao/Addax/releases), or you can build it yourself from source code.

### Installation

Unzip the downloaded installation package to the directory where you want to install it:

```bash
tar -xzf addax-{version}.tar.gz
```

### Environment Requirements

- Linux or macOS operating system
- Java 8 or higher version
- Python 3.6 or higher version (required for some plugins)

## First Synchronization Job

Let's start with a simple example - synchronizing data from a text file to another text file.

### Prepare Test Data

Create a test data file:

```bash
echo -e "1,zhangsan,20\n2,lisi,21\n3,wangwu,22" > /tmp/test.csv
```

### Create Job Configuration

Create a job configuration file `job.json`:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/tmp/test.csv",
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
              }
            ],
            "fieldDelimiter": ","
          }
        },
        "writer": {
          "name": "txtfilewriter",
          "parameter": {
            "path": "/tmp/result.csv",
            "fileName": "result",
            "writeMode": "truncate",
            "encoding": "UTF-8",
            "fieldDelimiter": ",",
            "nullFormat": "\\N"
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

### Execute Job

Run the synchronization job:

```bash
bin/addax.sh job.json
```

If successful, you should see output similar to:

```
2023-12-07 10:30:01.234 [main] INFO  JobContainer - Job ID: 202312071030, Total records: 3, Speed: 3rec/s (30B/s), Error records: 0
```

And you should find the result file at `/tmp/result.csv`.

## Database Synchronization Example

Here's a more practical example - synchronizing data from MySQL to PostgreSQL.

### Prerequisites

- MySQL database with test data
- PostgreSQL database for destination

### Create Job Configuration

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "mysql_user",
            "password": "mysql_password",
            "column": ["id", "name", "age"],
            "splitPk": "id",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/test",
                "table": ["user_table"]
              }
            ]
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "username": "postgres_user",
            "password": "postgres_password",
            "column": ["id", "name", "age"],
            "connection": [
              {
                "jdbcUrl": "jdbc:postgresql://localhost:5432/test",
                "table": ["user_table"]
              }
            ]
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

### Execute Job

```bash
bin/addax.sh mysql_to_postgresql.json
```

## Next Steps

- Learn more about [job configuration](setupJob.md)
- Explore available [reader plugins](reader/)
- Explore available [writer plugins](writer/)
- Learn about [performance tuning](debug.md)