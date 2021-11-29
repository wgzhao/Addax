/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.common.base;

/**
 * The common configuration items
 * If the plugin requires additional configuration items, you can create a new class to extend it
 */
public class Key
{
    // The connection item name. string type
    public static final String CONNECTION = "connection";
    // The columns of reading or writing. list type
    public static final String COLUMN = "column";
    // The field's name. string type
    public static final String NAME = "name";
    // The field's position. string/numeric type
    public static final String INDEX = "index";
    // The field's data type. string type
    public static final String TYPE = "type";
    // The field's value. string/numeric type
    public static final String VALUE = "value";
    // Compression alg will be read or write, default is NONE. string type
    public static final String COMPRESS = "compress";
    // data encoding，default is UTF-8. string type
    public static final String ENCODING = "encoding";
    // JDBC driver class name , in most cases, the program can guess automatically, without manual configuration. string type
    public static final String JDBC_DRIVER = "driver";
    // JDBC url. list type
    public static final String JDBC_URL = "jdbcUrl";
    // The url end point, mainly http-style reader/writer. string type
    public static final String ENDPOINT = "endpoint";
    // the user to connect source/destination endpoint. string type
    public static final String USERNAME = "username";
    // the password for user, can be empty. string type
    public static final String PASSWORD = "password";
    // The database name will be reader or write. string type
    public static final String DATABASE = "database";
    // the table name will be read or write. list type
    public static final String TABLE = "table";
    // SQL query clause, list type. list type
    public static final String WHERE = "where";
    // The SQL statement to be executed after the task is completed. list type
    public static final String POST_SQL = "postSql";
    // The SQL statements to be executed before task execution. list type
    public static final String PRE_SQL = "preSql";
    // Use query statements instead of specifying tables to get data
    // This configuration item and TABLE configuration item are mutually exclusive, list type
    public static final String QUERY_SQL = "querySql";
    // The primary key will be split. string type
    public static final String SPLIT_PK = "splitPk";
    // Auto guess table's split primary key, boolean type
    public static final String AUTO_PK = "autoPk";
    // The split number for each table, if primary key is present. numeric type
    public static final String EACH_TABLE_SPLIT_SIZE = "eachTableSplitSize";
    // Whether dry run or not ? boolean type
    public static final String DRY_RUN = "dryRun";
    // The max size each batch in rdbms reading, default is 2048. numeric type
    public static final String FETCH_SIZE = "fetchSize";
    // The max bytes each batch , numeric type
    public static final String BATCH_BYTE_SIZE = "batchByteSize";
    // The max number of records each batch, numeric type
    public static final String BATCH_SIZE = "batchSize";
    // The buffer size of reading or writing file, numeric type
    public static final String BUFFER_SIZE = "bufferSize";
    // Specify date type's format, default is 'yyyy-MM-dd hh:mm:ss', string type
    public static final String DATE_FORMAT = "dateFormat";
    // The file format will be read or write. string type
    public static final String FORMAT = "format";
    // Specify how to handle existing data if present, can choose to append, overwrite, nonconflict, string type
    public static final String WRITE_MODE = "writeMode";
    // The data path of reading or writing, string type
    public static final String PATH = "path";
    // The delimiter between fields, default is ','. list type
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // The filename will be read or write. string type
    public static final String FILE_NAME = "fileName";
    // The source files. list type
    public static final String SOURCE_FILES = "sourceFiles";
    // The file format will be read from or write to, it used on txtfilewriter/txtfilereader plugin. string type
    public static final String FILE_FORMAT = "fileFormat";
    // The hadoop HDFS defaultFS name, it requires on hdfsreader/hdfswriter plugins. string type
    public static final String DEFAULT_FS = "defaultFS";
    // The file type will be read from or write to hadoop hdfs, such as ORC, Parquet, Text etc. string type.
    public static final String FILE_TYPE = "fileType";
    // How to present null, by default, NULL is stored as \N. string type
    public static final String NULL_FORMAT = "nullFormat";
    // The record count, mainly used for streamreader plugin. numeric type
    public static final String SLICE_RECORD_COUNT = "sliceRecordCount";

    // Kerberos
    // Whether to configure kerberos. boolean type
    public static final String HAVE_KERBEROS = "haveKerberos";
    // The file path of kerberos keytab. string type
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    // The kerberos principal. string type
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    // The hadoop extra config, it used mainly in hadoop HDFS HA environments. map type
    public static final String HADOOP_CONFIG = "hadoopConfig";
    // The extra csv file reading configuration. map type
    public static final String CSV_READER_CONFIG = "csvReaderConfig";
    // Whether skip csv/tsv header or not. default is false. boolean type
    public static final String SKIP_HEADER = "skipHeader";

    // For decimal type
    // The max precision of decimal. numeric type
    public static final String PRECISION = "precision";
    // The max scale of decimal. numeric type
    public static final String SCALE = "scale";

    // For char type
    public static final String LENGTH = "length";


    // For Oracle reader ONLY
    public static final String HINT = "hint";
    public static final String SAMPLE_PERCENTAGE = "samplePercentage";
    // For RDBMS reader or write, configure extra jdbc connection session. map type
    public static final String SESSION = "session";

    // For FTP Writer ONLY
    public static final String SUFFIX = "suffix";

    // DOES NOT configure
    public static final String COLUMN_LIST = "columnList";
    public static final String SPLIT_PK_SQL = "splitPkSql";
    public static final String EMPTY_AS_NULL = "emptyAsNull";
    public static final String MANDATORY_ENCODING = "mandatoryEncoding";
    public static final String HEADER = "header";
    public static final String IS_TABLE_MODE = "isTableMode";

    public Key()
    {

    }
}