# Changelog

## 3.1.5

### General Changes

* Various code clean

### DBF reader

* Reconstruct this plugin with 3rd-party jar package
* Add support for `Date` type
* Fix for the occasional null pointer exception

### DBF writer

* Add support for `Date` type

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

