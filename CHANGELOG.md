# Changelog

## 3.1.1 

### General

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

## 3.1.2

### General

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