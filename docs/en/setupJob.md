# Job Configuration

This document details how to configure Addax synchronization jobs. Addax uses JSON format configuration files to describe synchronization tasks.

## Configuration Structure

A complete job configuration file consists of three main parts:

- **core**: Core configuration
- **job**: Job configuration 
- **setting**: Runtime settings

Here's the basic structure:

```json
{
  "core": {
    "transport": {
      "channel": {
        "speed": {
          "byte": 1048576
        }
      }
    }
  },
  "job": {
    "content": [
      {
        "reader": {},
        "writer": {}
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

## Core Configuration

The `core` section contains system-level configuration:

### Transport Configuration

```json
{
  "core": {
    "transport": {
      "channel": {
        "speed": {
          "byte": 1048576,
          "record": -1
        },
        "flowControlInterval": 20,
        "capacity": 512,
        "byteCapacity": 67108864
      }
    }
  }
}
```

**Parameters:**

- `speed.byte`: Byte-level speed limit (bytes per second), -1 means no limit
- `speed.record`: Record-level speed limit (records per second), -1 means no limit  
- `flowControlInterval`: Flow control check interval (milliseconds)
- `capacity`: Channel capacity (number of records)
- `byteCapacity`: Channel byte capacity

## Job Configuration

The `job` section contains the main synchronization task configuration:

### Content Array

The `content` array can contain multiple reader-writer pairs for complex synchronization scenarios:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {}
        },
        "writer": {
          "name": "postgresqlwriter", 
          "parameter": {}
        }
      }
    ]
  }
}
```

### Reader Configuration

Each reader must specify:

- `name`: Reader plugin name
- `parameter`: Reader-specific parameters

Example MySQL reader:

```json
{
  "reader": {
    "name": "mysqlreader",
    "parameter": {
      "username": "root",
      "password": "password",
      "column": ["id", "name", "age"],
      "splitPk": "id",
      "connection": [
        {
          "jdbcUrl": "jdbc:mysql://localhost:3306/test",
          "table": ["user_table"]
        }
      ]
    }
  }
}
```

### Writer Configuration

Each writer must specify:

- `name`: Writer plugin name
- `parameter`: Writer-specific parameters

Example PostgreSQL writer:

```json
{
  "writer": {
    "name": "postgresqlwriter",
    "parameter": {
      "username": "postgres",
      "password": "password", 
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
```

## Setting Configuration

The `setting` section controls job execution behavior:

### Speed Control

```json
{
  "setting": {
    "speed": {
      "channel": 3,
      "byte": 1048576,
      "record": 10000
    }
  }
}
```

**Parameters:**

- `channel`: Number of parallel channels (concurrency level)
- `byte`: Byte-level speed limit per second
- `record`: Record-level speed limit per second

### Error Control

```json
{
  "setting": {
    "errorLimit": {
      "record": 0,
      "percentage": 0.02
    }
  }
}
```

**Parameters:**

- `record`: Maximum allowed error records
- `percentage`: Maximum allowed error percentage

## Data Type Mapping

Addax uses a unified internal type system for data conversion:

| Addax Type | Description | Java Type |
|-----------|-------------|-----------|
| long | Long integer | java.lang.Long |
| double | Double precision float | java.lang.Double |
| string | String | java.lang.String |
| date | Date/time | java.util.Date |
| bool | Boolean | java.lang.Boolean |
| bytes | Byte array | byte[] |

## Variable Substitution

Addax supports variable substitution in configuration files:

```json
{
  "reader": {
    "parameter": {
      "jdbcUrl": "jdbc:mysql://${host}:${port}/${database}",
      "username": "${username}",
      "password": "${password}"
    }
  }
}
```

Variables can be passed via command line:

```bash
bin/addax.sh job.json -p "-Dhost=localhost -Dport=3306"
```

## Configuration Examples

### Simple File Transfer

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "txtfilereader",
          "parameter": {
            "path": "/tmp/input.txt",
            "encoding": "UTF-8",
            "column": ["*"],
            "fieldDelimiter": "\t"
          }
        },
        "writer": {
          "name": "txtfilewriter",
          "parameter": {
            "path": "/tmp/output",
            "fileName": "result.txt",
            "encoding": "UTF-8",
            "fieldDelimiter": "\t"
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

### Database to Database

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "source_user",
            "password": "source_pass",
            "column": ["id", "name", "email", "created_time"],
            "splitPk": "id",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://source-host:3306/source_db",
                "table": ["users"]
              }
            ]
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "username": "target_user",
            "password": "target_pass",
            "column": ["id", "name", "email", "created_time"],
            "connection": [
              {
                "jdbcUrl": "jdbc:postgresql://target-host:5432/target_db",
                "table": ["users"]
              }
            ]
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 5,
        "byte": 10485760
      },
      "errorLimit": {
        "record": 10,
        "percentage": 0.1
      }
    }
  }
}
```

For plugin-specific configuration details, please refer to the respective plugin documentation in the [reader](reader/) and [writer](writer/) sections.