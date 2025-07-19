# MySQL Reader

The MySQLReader plugin enables reading data from MySQL databases.

## Example

Let's create a table in MySQL's test database and insert a record:

```sql
CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT,
    salary DECIMAL(10,2),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO test_table (name, age, salary) VALUES 
('John Doe', 30, 50000.00),
('Jane Smith', 25, 45000.00);
```

Here's a configuration to read from this table to the console:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "password",
            "column": ["id", "name", "age", "salary", "created_time"],
            "splitPk": "id",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC",
                "table": ["test_table"]
              }
            ]
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

Save this configuration as `job/mysql2stream.json`.

### Execute the Job

Run the following command to execute the data collection:

```bash
bin/addax.sh job/mysql2stream.json
```

## Parameter Description

This plugin is based on the [RDBMS Reader](rdbmsreader.md) implementation, so you can refer to all configuration items of RDBMS Reader.

### driver

The current Addax uses MySQL JDBC driver version 8.0 or higher, with the driver class name `com.mysql.cj.jdbc.Driver`, not `com.mysql.jdbc.Driver`. If you need to collect from a MySQL server lower than version `5.6` and need to use the `Connector/J 5.1` driver, you can follow these steps:

**Replace the built-in driver**

```bash
rm -f plugin/reader/mysqlreader/libs/mysql-connector-java-*.jar
```

**Copy the old driver to the plugin directory**

```bash
cp mysql-connector-java-5.1.48.jar plugin/reader/mysqlreader/libs/
```

**Specify the driver class name**

In your JSON file, configure `"driver": "com.mysql.jdbc.Driver"`

### Required Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| jdbcUrl | JDBC connection URL | Yes | None |
| username | Database username | Yes | None |
| password | Database password | Yes | None |
| table | List of tables to read from | Yes | None |
| column | List of columns to read | Yes | None |

### Optional Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| splitPk | Primary key for data splitting | No | None |
| where | WHERE clause for filtering | No | None |
| querySql | Custom SQL query | No | None |
| fetchSize | JDBC fetch size | No | 1024 |
| driver | JDBC driver class name | No | com.mysql.cj.jdbc.Driver |

## Data Type Mapping

| MySQL Type | Addax Type | Notes |
|------------|------------|-------|
| TINYINT, SMALLINT, MEDIUMINT, INT | long | |
| BIGINT | long | |
| FLOAT, DOUBLE, DECIMAL | double | |
| VARCHAR, CHAR, TEXT | string | |
| DATE, TIME, DATETIME, TIMESTAMP | date | |
| BIT | bool | |
| BINARY, VARBINARY, BLOB | bytes | |

## Performance Tuning

### Split Key Configuration

For large tables, configure `splitPk` to enable parallel reading:

```json
{
  "parameter": {
    "splitPk": "id",
    "channel": 4
  }
}
```

### Fetch Size Optimization

Adjust `fetchSize` based on your memory and network conditions:

```json
{
  "parameter": {
    "fetchSize": 2048
  }
}
```

### Query Optimization

Use `where` clause to filter data at the source:

```json
{
  "parameter": {
    "where": "created_time >= '2023-01-01' AND status = 'active'"
  }
}
```

## Error Handling

Common issues and solutions:

### Connection Timeout

```json
{
  "parameter": {
    "jdbcUrl": "jdbc:mysql://localhost:3306/test?connectTimeout=60000&socketTimeout=60000"
  }
}
```

### SSL Connection Issues

```json
{
  "parameter": {
    "jdbcUrl": "jdbc:mysql://localhost:3306/test?useSSL=false"
  }
}
```

### Timezone Issues

```json
{
  "parameter": {
    "jdbcUrl": "jdbc:mysql://localhost:3306/test?serverTimezone=UTC"
  }
}
```

For more detailed configuration options, please refer to the [RDBMS Reader](rdbmsreader.md) documentation.