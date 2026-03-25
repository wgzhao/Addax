# Doris Reader

The DorisReader plugin reads data from Apache Doris and supports two connection modes:

- `jdbc:arrow-flight-sql`: Use Doris Arrow Flight SQL protocol (recommended)
- `jdbc:mysql`: Fallback to the MySQL-compatible protocol

If the `jdbcUrl` does not start with either prefix, the plugin will fail fast.

## Example

The following job uses Arrow Flight SQL to read from Doris and print to the console:

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3,
        "bytes": -1
      }
    },
    "content": {
      "reader": {
        "name": "dorisreader",
        "parameter": {
          "username": "root",
          "password": "root",
          "column": [
            "*"
          ],
          "connection": {
            "table": [
              "addax_reader"
            ],
            "jdbcUrl": "jdbc:arrow-flight-sql://127.0.0.1:9030?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false"
          }
        }
      },
      "writer": {
        "name": "streamwriter",
        "parameter": {
          "print": true
        }
      }
    }
  }
}
```

Save it as `job/doris2stream.json` and run:

```shell
bin/addax.sh job/doris2stream.json
```

## Parameters

This plugin is based on the [RDBMS Reader](rdbmsreader.md), so all RDBMS Reader configurations are supported.

### jdbcUrl

- Arrow Flight SQL:
  `jdbc:arrow-flight-sql://<fe_host>:<port>?useServerPrepStmts=false&cachePrepStmts=true&useSSL=false&useEncryption=false`
- MySQL compatible:
  `jdbc:mysql://<fe_host>:<port>/<db>`

### Mode Selection

- `jdbc:arrow-flight-sql` enables the Arrow Flight SQL protocol.
- `jdbc:mysql` follows the MySQLReader behavior for compatibility.

### JVM Requirement

When running Arrow Flight SQL on Java 9+, add the following JVM option:

```shell
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Notes

- Arrow Flight SQL and MySQL protocol parameters differ. Refer to the Doris documentation for details.
