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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The default value of the configuration item
 * If the plugin requires additional configuration items' value, you can create a new class to extend it
 */
public class Constant
{
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String DEFAULT_FILE_FORMAT = "text";
    public static final boolean DEFAULT_SKIP_HEADER = false;
    public static final char DEFAULT_FIELD_DELIMITER = ',';
    public static final String DEFAULT_NULL_FORMAT = "\\N";

    public static final int DEFAULT_BATCH_BYTE_SIZE = 32 * 1024 * 1024;
    public static final int DEFAULT_BATCH_SIZE = 2048;
    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final int DEFAULT_DECIMAL_PRECISION = 38;
    public static final int DEFAULT_DECIMAL_SCALE = 10;
    public static final int DEFAULT_EACH_TABLE_SPLIT_SIZE = 5;
    public static final int DEFAULT_FETCH_SIZE = 2048;
    public static final int DEFAULT_DECIMAL_MAX_PRECISION = 38;
    public static final int DEFAULT_DECIMAL_MAX_SCALE = 18;
    public static final String INSERT_OR_REPLACE_TEMPLATE_MARK = "insertOrReplaceTemplate";
    public static final String QUERY_SQL_TEMPLATE = "SELECT %s FROM %s WHERE (%s)";
    public static final String QUERY_SQL_TEMPLATE_WITHOUT_WHERE = "SELECT %s FROM %s ";
    public static final String TABLE_NAME_PLACEHOLDER = "@table";

    public static final String ENC_PASSWORD_PREFIX = "${enc:";

    public static final Set<String> SUPPORTED_WRITE_MODE = new HashSet<>(Arrays.asList("append", "nonConflict", "overwrite", "truncate"));

    public static final String SQL_FORMAT = "sql";
    public static final Set<String> SUPPORTED_FILE_FORMAT = new HashSet<>(Arrays.asList("csv", "text", "sql"));

    public static final Set<String> SQL_RESERVED_WORDS = new HashSet<>(Arrays.asList("ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION",
            "BACKUP", "BEFORE", "BETWEEN", "BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CAST", "CATALOG", "CHECK", "CHECKPOINT",
            "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT", "COMPUTE", "CONSTRAINT", "CONTAINS", "CONTINUE", "CONVERT",
            "CREATE", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATE", "DAY",
            "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DESC", "DISK", "DISTINCT", "DISTRIBUTED", "DO", "DROP", "DUMP", "ELSE", "END",
            "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "FETCH", "FILE", "FOR", "FOREIGN", "FREETEXT",
            "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING", "HOLDLOCK", "IDENTITY", "IDENTITYCOL",
            "IDENTITY_INSERT", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL", "LEFT", "LIKE",
            "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN",
            "OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT", "PLAN",
            "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES",
            "REPLICATION", "RESTORE", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA",
            "SECURITYAUDIT", "SELECT", "SEMANTICKEYPHRASETABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME", "STATISTICS",
            "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT",
            "TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN",
            "WHERE", "WHILE", "WITH", "WRITETEXT", "XACT_ABORT"));

    //用于插件对自身 split 的每个 task 标识其使用的资源，以告知core 对 reader/writer split 之后的 task 进行拼接时需要根据资源标签进行更有意义的 shuffle 操作
    public static final String LOAD_BALANCE_RESOURCE_MARK = "loadBalanceResourceMark";
}
