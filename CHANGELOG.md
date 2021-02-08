# Changelog

## 3.1.10

### General Changes

* Remove unnecessary jar packages

### RDBMS Reader

* Allow specify jdbc driver class name ([\#164c](https://github.com/wgzhao/DataX/commit/164cdeae521e4be1f9eb441b6a00e26efea933e4))

### RDBMS Writer

* Allow specify jdbc driver class name ([\#164c](https://github.com/wgzhao/DataX/commit/164cdeae521e4be1f9eb441b6a00e26efea933e4))

## 3.1.9

### General Changes

* Add HTTP reader plugin
* make `package.xml` more general

## 3.1.8

### General Changes

* Add Kudu reader plugin
* Add kudu writer plugin
* Add Greenplum writer plugin
* Add InfluxDB reader plugin
* Add InfluxDB Writer plugin  
* Change loglevel to reduce console output
* Add more exit code

### HDFS writer

* Fixed Date type can not convert to bytes

## 3.1.7

### General Changes

* Add support for more data type such as `Array`, `UUID`, `JSON`, `XML` 
* Add more deme job files
* Add support for HBase 2.0 reader ( `hbase20xreader` )
* Fixes for some minor issues
* Update some sample job json-string for documentation

### ClickHouse reader

* Add support for `DateTime`, `DateTime64` data type

### ClickHouse writer

* Add support for `DateTime`, `DateTime64` data type

### PostgreSQL reader

* Add support for `UUID`,`JSON`, `INET`,`MACADDR` data type

### PostgreSQL writer

* Add support for `UUID`,`JSON`, `INET`,`MACADDR` data type

### SQLServer reader

* Add support for `image` data type

### SQLServer writer

* Add support for `image` data type

### Oracle writer

* Fixed incorrect column cuased by duplicate variable initialization ([\#92](https://github.com/wgzhao/DataX/issues/92))

### Transformer

* Add power expressions (`^`) for `dx_map` function

## 3.1.6

### General Changes

* Add tar.gz release package
* Takes item `password` to optional instead of  required one

### Redis writer

* Fixed incorrect target packaging folder ([\#81](https://github.com/wgzhao/DataX/issues/81))

### Oracle writer

* Add support for `merge into` statement via configuring `wirteMode` item ([\#82](https://github.com/wgzhao/DataX/issues/81))

### PostgreSQL writer

* Add support for `insert into ... on conflict` statement via configuring `wirteMode` item ([\#82](https://github.com/wgzhao/DataX/issues/81))

## 3.1.5

### General Changes

* Various code clean

### DBF reader

* Reconstruct this plugin with 3rd-party jar package
* Add support for `Date` type
* Fix for the occasional null pointer exception

### DBF writer

* Add support for `Date` type

### Elasticsearch writer

* Fixed missing dependency jars ([\#68](https://github.com/wgzhao/DataX/issues/68))

### HDFS reader

* Fix read failure when the filetype is text ([\#66](https://github.com/wgzhao/DataX/issues/66), [\#68](https://github.com/wgzhao/DataX/issues/68))

## 3.1.4

This is an emergency fix version to fix a serious problem in a previous release ( [\#62](https://github.com/wgzhao/DataX/issues/62)).

## 3.1.3

### Redis reader

* Delete temporary local file
* Only parse redis `String` data type, other types will be ignore

### HDFS reader

* Add support for reading Parquet file (#54)

## 3.1.2

### General Changes

* Does not parse the `-m` command line argument, it doesn't really do anything!

### SQLServer reader

* Add support for `datetime` data type with old SQLServer edition

### HBase11xsql writer

* Add support for truncating table before inserting records
* Add support for specifing zookeeper's port, if not presents, it uses `2181` as default port 

### HDFS writer

* Add support for `timestamp` data type

### MongoDB reader

* Add support for `json` data type

## 3.1.1 

### General Changes

* Transformer add column's basic operation 
* Use prestosql's hadoop and hive jars instead of apache's
* various misc codes optimize
 
### dbffilereader

* remove supported for reading compressed dbf file
    
### jsonreader
 
* fixed parse non-string type value
  
### dbffilewriter
 
* fixed boolean type writing error
   
### hdfswriter

*  Use keyword `parquest` indicates support parquet format,  old keyword `par` is not used

