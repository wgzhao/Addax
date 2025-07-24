# PostgreSQL Reader

The PostgreSQLReader plugin enables reading data from PostgreSQL databases.

## Example

Create a sample table in PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (username, email, age) VALUES 
('alice', 'alice@example.com', 28),
('bob', 'bob@example.com', 32),
('charlie', 'charlie@example.com', 25);
```

Configuration to read from PostgreSQL:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "postgres",
            "password": "password",
            "column": ["id", "username", "email", "age", "created_at"],
            "splitPk": "id",
            "connection": [
              {
                "jdbcUrl": "jdbc:postgresql://localhost:5432/testdb",
                "table": ["users"]
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

## Parameters

This plugin is based on the [RDBMS Reader](rdbmsreader.md) implementation.

### Required Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| jdbcUrl | PostgreSQL JDBC connection URL | Yes | None |
| username | Database username | Yes | None |
| password | Database password | Yes | None |
| table | List of tables to read from | Yes | None |
| column | List of columns to read | Yes | None |

### Optional Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| splitPk | Primary key for data splitting | No | None |
| where | WHERE clause for filtering | No | None |
| fetchSize | JDBC fetch size | No | 1024 |

## Data Type Mapping

| PostgreSQL Type | Addax Type | Notes |
|-----------------|------------|-------|
| SMALLINT, INTEGER, BIGINT | long | |
| REAL, DOUBLE PRECISION, NUMERIC | double | |
| VARCHAR, CHAR, TEXT | string | |
| DATE, TIME, TIMESTAMP | date | |
| BOOLEAN | bool | |
| BYTEA | bytes | |

## Performance Tips

### Use Split Key for Large Tables

```json
{
  "parameter": {
    "splitPk": "id",
    "setting": {
      "speed": {
        "channel": 4
      }
    }
  }
}
```

### Optimize with WHERE Clause

```json
{
  "parameter": {
    "where": "created_at >= '2023-01-01' AND status = 'active'"
  }
}
```

## Connection Examples

### Standard Connection

```json
{
  "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb"
}
```

### SSL Connection

```json
{
  "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb?ssl=true&sslmode=require"
}
```

### Connection Pool Settings

```json
{
  "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb?prepareThreshold=0&preparedStatementCacheQueries=0"
}
```