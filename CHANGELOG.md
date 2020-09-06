# Changelog

## 3.1.1 

### General

* Transformer add column's basic operation 
* Use prestosql's hadoop and hive jars instead of apache's
 
### dbffilereader

* remove supported for reading compressed dbf file
    
### jsonreader
 
* fixed parse non-string type value
  
### dbffilewriter
 
* fixed boolean type writing error
   
### hdfswriter

*  Use keyword `parquest` indicates support parquet format,  old keyword `par` is not used