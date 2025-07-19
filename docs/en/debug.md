# Debug Mode

Addax provides debugging capabilities to help troubleshoot issues during data synchronization jobs.

## Enabling Debug Mode

### Command Line Debug

Enable debug mode by adding JVM parameters:

```bash
bin/addax.sh -j "-Daddax.debug=true" job.json
```

### Configuration Debug

Add debug configuration in your job file:

```json
{
  "job": {
    "setting": {
      "debug": true
    }
  }
}
```

## Debug Features

### Detailed Logging

Debug mode provides more detailed logging including:

- SQL statements being executed
- Data transformation details
- Performance metrics
- Error stack traces

### Data Sampling

When debug mode is enabled, Addax will log sample data:

```bash
bin/addax.sh -j "-Daddax.debug=true -Daddax.debug.sample=10" job.json
```

This will log the first 10 records for inspection.

### Memory Monitoring

Monitor memory usage during execution:

```bash
bin/addax.sh -j "-Daddax.debug=true -Daddax.debug.memory=true" job.json
```

## Common Debug Scenarios

### Connection Issues

If you're experiencing connection problems:

```bash
bin/addax.sh -j "-Daddax.debug=true -Daddax.debug.connection=true" job.json
```

### Performance Issues

For performance debugging:

```bash
bin/addax.sh -j "-Daddax.debug=true -Daddax.debug.performance=true" job.json
```

### Data Type Issues

For data type conversion problems:

```bash
bin/addax.sh -j "-Daddax.debug=true -Daddax.debug.datatype=true" job.json
```

## Log Levels

Set different log levels for various components:

```bash
bin/addax.sh -j "-Dlogback.configurationFile=conf/logback-debug.xml" job.json
```

## Debug Output Examples

### SQL Execution Debug

```
DEBUG [Reader-0] - Executing SQL: SELECT id, name, age FROM users WHERE id BETWEEN ? AND ?
DEBUG [Reader-0] - SQL Parameters: [1, 1000]
DEBUG [Reader-0] - Fetched 856 records in 1.23 seconds
```

### Data Sample Debug

```
DEBUG [Channel-0] - Sample record: {"id": 1, "name": "John Doe", "age": 30}
DEBUG [Channel-0] - Sample record: {"id": 2, "name": "Jane Smith", "age": 25}
```

### Performance Debug

```
DEBUG [Job] - Channel statistics:
  - Channel 0: 1000 records/s, 128KB/s
  - Channel 1: 950 records/s, 122KB/s
  - Channel 2: 1050 records/s, 135KB/s
```

## Troubleshooting Tips

### High Memory Usage

Monitor memory usage and adjust heap size:

```bash
bin/addax.sh -j "-Xms2g -Xmx8g -Daddax.debug.memory=true" job.json
```

### Slow Performance

Identify bottlenecks:

```bash
bin/addax.sh -j "-Daddax.debug.performance=true -Daddax.debug.channel=true" job.json
```

### Data Quality Issues

Check for data conversion errors:

```bash
bin/addax.sh -j "-Daddax.debug.datatype=true -Daddax.debug.sample=100" job.json
```

## Custom Debug Configuration

Create a custom logback configuration for specific debug needs:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <logger name="com.wgzhao.addax" level="DEBUG"/>
    <logger name="com.wgzhao.addax.core.job" level="TRACE"/>
    
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

Save as `conf/logback-custom.xml` and use:

```bash
bin/addax.sh -j "-Dlogback.configurationFile=conf/logback-custom.xml" job.json
```

## Production Debugging

For production environments, use selective debugging:

```bash
# Only log errors and warnings
bin/addax.sh -j "-Daddax.debug.errors=true" job.json

# Log performance metrics only
bin/addax.sh -j "-Daddax.debug.performance=true" job.json
```

This helps identify issues without overwhelming log output.