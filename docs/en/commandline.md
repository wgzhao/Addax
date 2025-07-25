# Command Line Usage

Addax provides a simple command-line interface for executing data synchronization jobs.

## Basic Syntax

```bash
bin/addax.sh [options] job_config_file
```

## Command Line Options

### `-h, --help`

Display help information and exit.

```bash
bin/addax.sh -h
```

### `-v, --version`

Display version information and exit.

```bash
bin/addax.sh -v
```

### `-m, --mode`

Set execution mode. Available options:

- `standalone`: Standalone mode (default)
- `local`: Local mode

```bash
bin/addax.sh -m standalone job.json
```

### `-j, --jvm`

Set JVM parameters.

```bash
bin/addax.sh -j "-Xms1g -Xmx4g" job.json
```

### `-p, --params`

Pass runtime parameters for variable substitution in job configuration.

```bash
bin/addax.sh -p "-Dhost=localhost -Dport=3306" job.json
```

### `--reader-plugin`

Display information about a specific reader plugin.

```bash
bin/addax.sh --reader-plugin mysqlreader
```

### `--writer-plugin`

Display information about a specific writer plugin.

```bash
bin/addax.sh --writer-plugin postgresqlwriter
```

## Usage Examples

### Basic Job Execution

Execute a simple synchronization job:

```bash
bin/addax.sh job/mysql_to_postgres.json
```

### Job with Custom JVM Settings

Execute job with custom memory settings:

```bash
bin/addax.sh -j "-Xms2g -Xmx8g -XX:+UseG1GC" job/large_table_sync.json
```

### Job with Runtime Parameters

Execute job with variable substitution:

```bash
bin/addax.sh -p "-Dsource.host=db1.example.com -Dtarget.host=db2.example.com" job/template.json
```

Where `template.json` contains variables like:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "parameter": {
            "jdbcUrl": "jdbc:mysql://${source.host}:3306/mydb"
          }
        },
        "writer": {
          "parameter": {
            "jdbcUrl": "jdbc:postgresql://${target.host}:5432/mydb"
          }
        }
      }
    ]
  }
}
```

### Debug Mode

Run job with debug output:

```bash
bin/addax.sh -j "-Daddax.debug=true" job.json
```

### Performance Monitoring

Run job with performance monitoring enabled:

```bash
bin/addax.sh -j "-Daddax.monitor=true" job.json
```

## Exit Codes

Addax uses the following exit codes:

- `0`: Job completed successfully
- `1`: Job failed due to configuration error
- `2`: Job failed due to runtime error
- `3`: Job killed by user or system

## Configuration Override

You can override configuration settings via command line parameters:

### Override Speed Settings

```bash
bin/addax.sh -p "-Djob.setting.speed.channel=5" job.json
```

### Override Error Limits

```bash
bin/addax.sh -p "-Djob.setting.errorLimit.record=100" job.json
```

## Plugin Information

### List Available Plugins

```bash
# List all reader plugins
bin/addax.sh --reader-plugin

# List all writer plugins  
bin/addax.sh --writer-plugin
```

### Get Plugin Details

```bash
# Get MySQL reader details
bin/addax.sh --reader-plugin mysqlreader

# Get PostgreSQL writer details
bin/addax.sh --writer-plugin postgresqlwriter
```

## Environment Variables

You can set the following environment variables to customize Addax behavior:

### `ADDAX_HOME`

Set the Addax installation directory:

```bash
export ADDAX_HOME=/opt/addax
```

### `JAVA_HOME`

Set the Java installation directory:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
```

### `ADDAX_OPTS`

Set default JVM options:

```bash
export ADDAX_OPTS="-Xms1g -Xmx4g"
```

## Logging Configuration

Addax uses logback for logging. You can customize logging by:

### Setting Log Level

```bash
bin/addax.sh -j "-Dlogback.configurationFile=conf/logback-debug.xml" job.json
```

### Custom Log File

```bash
bin/addax.sh -j "-Daddax.log.file=/var/log/addax/job.log" job.json
```

## Best Practices

### Resource Management

- Use appropriate JVM heap sizes based on your data volume
- Monitor memory usage during large data transfers
- Set reasonable channel numbers based on system capacity

### Error Handling

- Always check exit codes in scripts
- Set appropriate error limits for your use case
- Review logs for detailed error information

### Security

- Avoid passing passwords via command line (use configuration files)
- Use encrypted password files when possible
- Limit file permissions on configuration files

### Performance

- Test with different channel numbers to find optimal concurrency
- Use speed limits to prevent overwhelming source/target systems
- Monitor system resources during execution

For more detailed information about specific plugins and configuration options, please refer to the [job configuration guide](setupJob.md) and individual plugin documentation.