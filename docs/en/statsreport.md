# Statistics Report

Addax provides detailed statistics and monitoring during job execution to help you understand performance and troubleshoot issues.

## Overview

During execution, Addax reports various metrics including:

- Records processed per second
- Bytes transferred per second  
- Error counts and percentages
- Channel performance
- Memory usage
- Job progress

## Statistics Output

### Console Output

During job execution, Addax displays real-time statistics:

```
2023-12-07 10:30:15.123 [Statistics] INFO - Total records: 15000, Speed: 1500 rec/s (1.2 MB/s), Errors: 0 (0.00%)
2023-12-07 10:30:25.124 [Statistics] INFO - Total records: 30000, Speed: 1500 rec/s (1.2 MB/s), Errors: 2 (0.01%)
2023-12-07 10:30:35.125 [Statistics] INFO - Total records: 45000, Speed: 1500 rec/s (1.2 MB/s), Errors: 2 (0.00%)
```

### Final Report

At job completion, a comprehensive report is displayed:

```
====================Job Statistics====================
Job ID: 202312071030001
Start Time: 2023-12-07 10:30:00
End Time: 2023-12-07 10:35:30
Total Time: 5m30s

Records:
  - Total Read: 100000
  - Total Written: 99998
  - Errors: 2
  - Error Rate: 0.002%

Performance:
  - Average Speed: 303 rec/s
  - Peak Speed: 450 rec/s
  - Total Bytes: 12.5 MB
  - Throughput: 38.5 KB/s

Channels:
  - Channel 0: 25000 records, 310 rec/s
  - Channel 1: 25000 records, 305 rec/s  
  - Channel 2: 24999 records, 298 rec/s
  - Channel 3: 24999 records, 295 rec/s
========================================================
```

## Enabling Detailed Statistics

### Command Line Option

Enable detailed statistics reporting:

```bash
bin/addax.sh -j "-Daddax.stats.detailed=true" job.json
```

### Configuration Setting

Add to job configuration:

```json
{
  "job": {
    "setting": {
      "statistics": {
        "detailed": true,
        "interval": 10000
      }
    }
  }
}
```

## Statistics Components

### Performance Metrics

| Metric | Description | Unit |
|--------|-------------|------|
| Records/sec | Number of records processed per second | rec/s |
| Bytes/sec | Amount of data transferred per second | B/s |
| Error Rate | Percentage of failed records | % |
| Channel Utilization | Performance of each parallel channel | rec/s |

### Memory Statistics

```bash
bin/addax.sh -j "-Daddax.stats.memory=true" job.json
```

Output includes:
- JVM heap usage
- Memory allocation rates
- Garbage collection statistics

### I/O Statistics

Track input/output performance:

```bash
bin/addax.sh -j "-Daddax.stats.io=true" job.json
```

Includes:
- Network I/O rates
- Disk read/write speeds
- Connection pool statistics

## Custom Statistics Interval

### Set Update Frequency

```json
{
  "job": {
    "setting": {
      "statistics": {
        "interval": 5000
      }
    }
  }
}
```

### Disable Statistics

```json
{
  "job": {
    "setting": {
      "statistics": {
        "enabled": false
      }
    }
  }
}
```

## Export Statistics

### JSON Format

Export statistics to JSON file:

```bash
bin/addax.sh -j "-Daddax.stats.export=/tmp/job_stats.json" job.json
```

### CSV Format

Export to CSV for analysis:

```bash
bin/addax.sh -j "-Daddax.stats.export=/tmp/job_stats.csv -Daddax.stats.format=csv" job.json
```

## Monitoring Integration

### Prometheus Metrics

Enable Prometheus endpoint:

```bash
bin/addax.sh -j "-Daddax.metrics.prometheus.enabled=true -Daddax.metrics.prometheus.port=9090" job.json
```

### JMX Monitoring

Enable JMX for external monitoring tools:

```bash
bin/addax.sh -j "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999" job.json
```

## Performance Analysis

### Identifying Bottlenecks

Look for these patterns in statistics:

**Reader Bottleneck:**
```
Channel 0: 1000 rec/s (reader working hard)
Channel 1: 100 rec/s (waiting for reader)
Channel 2: 100 rec/s (waiting for reader)
```

**Writer Bottleneck:**
```
Channel 0: 200 rec/s (waiting for writer)
Channel 1: 200 rec/s (waiting for writer)  
Channel 2: 200 rec/s (waiting for writer)
```

**Network Bottleneck:**
```
High record rate but low byte rate indicates small records
Low record rate but high byte rate indicates large records
```

### Optimization Recommendations

Based on statistics, consider:

1. **Low Channel Utilization**: Increase channel count
2. **High Error Rate**: Check data quality and mappings
3. **Memory Issues**: Adjust JVM heap size
4. **Uneven Channel Performance**: Check data distribution

## Alerting

### Error Threshold Alerts

Set up alerts for error rates:

```json
{
  "job": {
    "setting": {
      "errorLimit": {
        "record": 100,
        "percentage": 0.1
      },
      "statistics": {
        "alerts": {
          "errorRate": 0.05
        }
      }
    }
  }
}
```

### Performance Alerts

Alert on low performance:

```json
{
  "job": {
    "setting": {
      "statistics": {
        "alerts": {
          "minSpeed": 100
        }
      }
    }
  }
}
```

## Troubleshooting with Statistics

### Common Issues

**Job Running Slowly:**
- Check channel utilization
- Monitor memory usage
- Verify network connectivity

**High Error Rate:**
- Review data type mappings
- Check source data quality
- Verify target constraints

**Memory Errors:**
- Monitor heap usage trends
- Check for memory leaks
- Adjust JVM settings

### Debug Commands

Get real-time statistics:

```bash
# Monitor job progress
tail -f $ADDAX_HOME/log/addax.log | grep Statistics

# Check JVM status
jstat -gc <pid>

# Monitor network connections
netstat -an | grep <port>
```

This comprehensive statistics system helps ensure your data synchronization jobs run efficiently and provides the visibility needed for troubleshooting and optimization.