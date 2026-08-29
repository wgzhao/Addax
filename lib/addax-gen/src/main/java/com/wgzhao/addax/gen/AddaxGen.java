/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.gen;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.util.EncryptUtil;
import com.wgzhao.addax.rdbms.util.SchemaProbe;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.wgzhao.addax.core.base.Constant.ENC_PASSWORD_PREFIX;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;

/**
 * Main entry for the {@code addax gen} command.
 * <p>
 * Supported scenarios:
 * <ul>
 *   <li>RDBMS to RDBMS: two JDBC connection strings, both sides probed and filled</li>
 *   <li>RDBMS to HDFS: a JDBC source and an {@code hdfs://host:port/path} target
 *       that fills the hdfswriter template (defaultFS, path, typed columns, and
 *       optional fileType/writeMode/compress/kerberos/hadoopConfig overrides)</li>
 * </ul>
 * Anything else is rejected with a pointer to the legacy {@code gen -r/-w}
 * template stitching.
 */
public class AddaxGen
{
    private static final String USAGE = """
            Usage: addax gen [options] <source-connection-string> --to <target>
              --to <connection-string>   target JDBC connection string, or hdfs://host:port/path (required)
              --table <table>            table name, once per side (default: same table)
              --columns a,b,c            explicit column list (default: all probed columns)
              --channel N                parallel channels (default: 1)
              --password-env VAR         read source password from environment variable
              --output <file>            write job to file instead of stdout
              --overwrite                allow overwriting an existing output file
              --no-probe                 skip schema probing, emit template skeleton
              -l                         list all reader/writer plugin names
            HDFS target options (only with --to hdfs://...):
              --file-type orc|parquet|text   output file type (default: orc)
              --write-mode append|overwrite|nonConflict (default: overwrite)
              --compress NONE|GZIP|SNAPPY|LZO|BZIP2 (default: SNAPPY)
              --field-delimiter <char>       text file delimiter (default: template default)
              --encoding <charset>           text file encoding
              --file-name <name>             output file name (default: source table name)
              --have-kerberos true|false     enable kerberos (default: false)
              --kerberos-principal <p>       kerberos principal (with --have-kerberos true)
              --kerberos-keytab <path>       kerberos keytab path (with --have-kerberos true)
              --hadoop-config k=v           extra hadoop config entry (repeatable, e.g. HA nameservices)
              --hdfs-site-path <path>       path to hdfs-site.xml (alternative to --hadoop-config)
            Connection string: scheme://user:password@host:port/database
            """;

    private String sourcePasswordEnv;
    private boolean overwrite;
    private boolean noProbe;
    private String outputFile;
    private int channel = 1;
    private List<String> explicitColumns;
    private final List<String> tables = new ArrayList<>();

    // HDFS target options
    private String fileType;
    private String writeMode;
    private String compress;
    private String fieldDelimiter;
    private String encoding;
    private String fileName;
    private boolean haveKerberos;
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private final List<String> hadoopConfigs = new ArrayList<>();
    private String hdfsSitePath;

    public static void main(String[] args)
    {
        int code = new AddaxGen().run(args);
        System.exit(code);
    }

    private int run(String[] args)
    {
        List<String> positional = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--to" -> positional.add("--to=" + requireValue(args, ++i, arg));
                case "--table" -> tables.add(requireValue(args, ++i, arg));
                case "--columns" -> explicitColumns = Arrays.asList(requireValue(args, ++i, arg).split(","));
                case "--channel" -> channel = parseInt(args, ++i);
                case "--password-env" -> sourcePasswordEnv = requireValue(args, ++i, arg);
                case "--output" -> outputFile = requireValue(args, ++i, arg);
                case "--overwrite" -> overwrite = true;
                case "--no-probe" -> noProbe = true;
                case "--file-type" -> fileType = requireValue(args, ++i, arg);
                case "--write-mode" -> writeMode = requireValue(args, ++i, arg);
                case "--compress" -> compress = requireValue(args, ++i, arg);
                case "--field-delimiter" -> fieldDelimiter = requireValue(args, ++i, arg);
                case "--encoding" -> encoding = requireValue(args, ++i, arg);
                case "--file-name" -> fileName = requireValue(args, ++i, arg);
                case "--have-kerberos" -> haveKerberos = Boolean.parseBoolean(requireValue(args, ++i, arg));
                case "--kerberos-principal" -> kerberosPrincipal = requireValue(args, ++i, arg);
                case "--kerberos-keytab" -> kerberosKeytab = requireValue(args, ++i, arg);
                case "--hadoop-config" -> hadoopConfigs.add(requireValue(args, ++i, arg));
                case "--hdfs-site-path" -> hdfsSitePath = requireValue(args, ++i, arg);
                case "-l" -> {
                    listPlugins();
                    return 0;
                }
                case "-h", "--help" -> {
                    System.out.println(USAGE);
                    return 0;
                }
                default -> {
                    if (arg.startsWith("-")) {
                        System.err.println("Unknown option: " + arg);
                        System.err.println(USAGE);
                        return 2;
                    }
                    positional.add(arg);
                }
            }
        }

        if (positional.isEmpty()) {
            System.err.println("Missing source connection string");
            System.err.println(USAGE);
            return 2;
        }
        if (!positional.stream().anyMatch(p -> p.startsWith("--to="))) {
            System.err.println("Missing --to target connection string");
            System.err.println(USAGE);
            return 2;
        }

        String sourceConn = positional.get(0);
        String targetConn = positional.stream().filter(p -> p.startsWith("--to=")).findFirst()
                .orElseThrow().substring("--to=".length());
        String sourceTable = tables.isEmpty() ? "" : tables.get(0);
        String targetTable = tables.size() > 1 ? tables.get(1) : sourceTable;
        if (sourceTable.isEmpty()) {
            System.err.println("Missing --table");
            System.err.println(USAGE);
            return 2;
        }

        try {
            generate(sourceConn, targetConn, sourceTable, targetTable);
            return 0;
        }
        catch (AddaxException e) {
            System.err.println("Error: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("Caused by: " + e.getCause().getMessage());
            }
            return 1;
        }
        catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return 2;
        }
    }

    private void generate(String sourceConnString, String targetConnString, String sourceTable, String targetTable)
    {
        // the source must be a JDBC connection string (RDBMS)
        if (!sourceConnString.contains("://")) {
            System.err.println("Error: the source must be a JDBC connection string (scheme://...)."
                    + " Only RDBMS-to-RDBMS and RDBMS-to-HDFS generation is supported;"
                    + " for other plugins use 'addax.sh gen -r <reader> -w <writer>'");
            return;
        }
        if (!isHdfsTarget(targetConnString) && !targetConnString.contains("://")) {
            System.err.println("Error: unsupported target '" + targetConnString + "'. Only RDBMS-to-RDBMS"
                    + " and RDBMS-to-HDFS generation is supported; for other plugins use"
                    + " 'addax.sh gen -r <reader> -w <writer>'");
            return;
        }
        ConnectionString source = ConnectionString.parse(sourceConnString);

        Path addaxHome = Path.of(System.getProperty("addax.home", "."));
        JSONObject readerTemplate = readTemplate(addaxHome, "reader", source.readerPlugin());
        JSONObject writerTemplate = isHdfsTarget(targetConnString)
                ? readTemplate(addaxHome, "writer", "hdfswriter")
                : readTemplate(addaxHome, "writer", ConnectionString.parse(targetConnString).writerPlugin());

        if (noProbe) {
            emit(readerTemplate, writerTemplate);
            return;
        }

        String sourcePassword = sourcePassword();
        if (sourcePassword == null) {
            sourcePassword = source.password();
        }

        // probe the source schema
        List<SchemaProbe.ColumnInfo> probedColumns = SchemaProbe.getColumns(
                source.dataBaseType(), source.toJdbcUrl(), source.username(), sourcePassword,
                source.database(), sourceTable);
        if (probedColumns.isEmpty()) {
            System.err.println("Error: source table '" + sourceTable + "' not found or has no columns");
            return;
        }
        List<String> sourceNames = probedColumns.stream().map(c -> c.name).toList();
        List<String> selectedColumns = explicitColumns == null ? sourceNames : validateColumns(sourceNames, explicitColumns);

        String splitPk = null;
        if (isHdfsTarget(targetConnString)) {
            fillHdfsWriter(writerTemplate, targetConnString, sourceTable, probedColumns, selectedColumns);
        }
        else {
            ConnectionString target = ConnectionString.parse(targetConnString);
            // verify the target table and warn about mismatches
            List<String> targetTables = SchemaProbe.listTables(
                    target.dataBaseType(), target.toJdbcUrl(), target.username(), target.password(),
                    target.database());
            if (!targetTables.contains(targetTable)) {
                System.err.println("Error: target table '" + targetTable + "' does not exist");
                suggestSimilar(targetTables, targetTable);
                return;
            }
            List<SchemaProbe.ColumnInfo> targetColumns = SchemaProbe.getColumns(
                    target.dataBaseType(), target.toJdbcUrl(), target.username(), target.password(),
                    target.database(), targetTable);
            Map<String, String> targetTypes = SchemaProbe.columnTypes(targetColumns);
            warnMissingColumns(selectedColumns, targetTypes);
            warnTypeMismatches(probedColumns, targetTypes);

            // primary key for splitPk when parallelism > 1
            if (channel > 1) {
                List<String> pk = SchemaProbe.getPrimaryKeys(
                        source.dataBaseType(), source.toJdbcUrl(), source.username(), sourcePassword,
                        source.database(), sourceTable);
                if (!pk.isEmpty()) {
                    splitPk = pk.get(0);
                }
            }

            applyToTemplate(writerTemplate, target, targetTable, selectedColumns, null, target.password());
        }

        applyToTemplate(readerTemplate, source, sourceTable, selectedColumns, splitPk, sourcePassword);

        emit(readerTemplate, writerTemplate);
    }

    private boolean isHdfsTarget(String target)
    {
        return target.startsWith("hdfs://");
    }

    /**
     * Fills the hdfswriter template from an {@code hdfs://host:port/path} target and the
     * probed source columns. Template sample values that only apply to specific setups
     * (kerberos, HA hadoopConfig, bloom filters) are removed unless explicitly requested.
     */
    private void fillHdfsWriter(JSONObject template, String hdfsTarget, String sourceTable,
            List<SchemaProbe.ColumnInfo> probedColumns, List<String> selectedColumns)
    {
        String rest = hdfsTarget.substring("hdfs://".length());
        int slash = rest.indexOf('/');
        if (slash <= 0 || slash == rest.length() - 1) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "Invalid HDFS target (expected hdfs://host:port/path): " + hdfsTarget);
        }
        String defaultFS = "hdfs://" + rest.substring(0, slash);
        // keep the leading slash: hdfs://host/tmp yields the absolute path /tmp
        String path = rest.substring(slash);

        JSONObject parameter = template.getJSONObject("parameter");
        parameter.put("defaultFS", defaultFS);
        parameter.put("path", path);
        parameter.put("fileName", fileName != null ? fileName : sourceTable);
        if (fileType != null) {
            parameter.put("fileType", fileType);
        }
        if (writeMode != null) {
            parameter.put("writeMode", writeMode);
        }
        if (compress != null) {
            parameter.put("compress", compress);
        }
        if (fieldDelimiter != null) {
            parameter.put("fieldDelimiter", fieldDelimiter);
        }
        if (encoding != null) {
            parameter.put("encoding", encoding);
        }

        // typed column list generated from the source schema
        JSONArray columnEntries = new JSONArray();
        for (SchemaProbe.ColumnInfo column : probedColumns) {
            if (!selectedColumns.contains(column.name)) {
                continue;
            }
            JSONObject entry = new JSONObject();
            entry.put("name", column.name);
            entry.put("type", mapColumnType(column.typeName));
            columnEntries.add(entry);
        }
        parameter.put("column", columnEntries);

        // kerberos defaults to off; the template's sample values are removed
        parameter.put("haveKerberos", Boolean.toString(haveKerberos));
        if (haveKerberos) {
            parameter.put("kerberosPrincipal", kerberosPrincipal != null ? kerberosPrincipal : "");
            parameter.put("kerberosKeytabFilePath", kerberosKeytab != null ? kerberosKeytab : "");
        }
        else {
            parameter.remove("kerberosPrincipal");
            parameter.remove("kerberosKeytabFilePath");
        }

        // HA configuration only when explicitly provided; otherwise drop the sample block
        if (!hadoopConfigs.isEmpty()) {
            JSONObject config = new JSONObject();
            for (String entry : hadoopConfigs) {
                int eq = entry.indexOf('=');
                if (eq <= 0) {
                    throw AddaxException.asAddaxException(CONFIG_ERROR,
                            "Invalid --hadoop-config entry (expected k=v): " + entry);
                }
                config.put(entry.substring(0, eq), entry.substring(eq + 1));
            }
            parameter.put("hadoopConfig", config);
        }
        else {
            parameter.remove("hadoopConfig");
        }

        // hdfs-site.xml path as an alternative to manual hadoopConfig entries
        if (hdfsSitePath != null) {
            parameter.put("hdfsSitePath", hdfsSitePath);
        }

        // bloom filters are not generated in v1; drop the template's sample columns
        parameter.remove("bloom.filter.columns");
        parameter.remove("bloom.filter.fpp");
    }

    /** Fills connection, credentials, columns and splitPk into a JDBC plugin template. */
    private void applyToTemplate(JSONObject template, ConnectionString conn, String table,
            List<String> columns, String splitPk, String password)
    {
        JSONObject parameter = template.getJSONObject("parameter");
        JSONObject connection = parameter.getJSONObject("connection");
        if (connection == null) {
            // some templates declare connection as an array of one object
            JSONArray connections = parameter.getJSONArray("connection");
            if (connections != null && !connections.isEmpty()) {
                connection = connections.getJSONObject(0);
            }
        }
        if (connection != null) {
            connection.put("jdbcUrl", conn.toJdbcUrl());
            connection.put("table", Collections.singletonList(table));
        }
        if (conn.username() != null) {
            parameter.put("username", conn.username());
        }
        parameter.put("password", encrypt(password != null ? password : ""));
        parameter.put("column", columns);
        if (splitPk != null && parameter.containsKey("splitPk")) {
            parameter.put("splitPk", splitPk);
        }
    }

    /**
     * Maps a JDBC type name to the Hive type string used in the hdfswriter column config
     * (SupportHiveDataType). Integer types are matched by exact name, because substring
     * matching would misclassify types like PostgreSQL 'interval' or 'point' as integers.
     * Hive has no TIME or INTERVAL type, so those fall back to STRING with a warning.
     */
    private String mapColumnType(String jdbcTypeName)
    {
        if (jdbcTypeName == null) {
            return "string";
        }
        String type = jdbcTypeName.toLowerCase(Locale.ROOT);
        if (type.contains("bool")) {
            return "boolean";
        }
        if (type.equals("tinyint")) {
            return "tinyint";
        }
        if (type.equals("smallint") || type.equals("int2")) {
            return "smallint";
        }
        if (type.equals("bigint") || type.equals("int8") || type.equals("serial8")) {
            return "bigint";
        }
        if (type.equals("int") || type.equals("int4") || type.equals("integer")
                || type.equals("mediumint") || type.equals("serial") || type.equals("serial4")) {
            return "int";
        }
        if (type.contains("char") || type.contains("text") || type.contains("uuid")
                || type.contains("json") || type.contains("bytea")) {
            return "string";
        }
        if (type.contains("double") || type.contains("float") || type.contains("real")
                || type.contains("money")) {
            return "double";
        }
        if (type.contains("decimal") || type.contains("numeric")) {
            return "decimal";
        }
        if (type.contains("timestamp")) {
            return "timestamp";
        }
        if (type.equals("date")) {
            return "date";
        }
        if (type.equals("time") || type.equals("timetz")) {
            System.err.println("Warning: column type '" + jdbcTypeName + "' has no Hive equivalent, mapped to string");
            return "string";
        }
        if (type.equals("interval")) {
            System.err.println("Warning: column type 'interval' has no Hive equivalent, mapped to string");
            return "string";
        }
        System.err.println("Warning: unmapped column type '" + jdbcTypeName + "' falls back to string");
        return "string";
    }

    private void emit(JSONObject readerTemplate, JSONObject writerTemplate)
    {
        JSONObject job = new JSONObject();
        JSONObject setting = new JSONObject();
        JSONObject speed = new JSONObject();
        speed.put("byte", -1);
        speed.put("channel", channel);
        setting.put("speed", speed);
        JSONObject content = new JSONObject();
        content.put("reader", readerTemplate);
        content.put("writer", writerTemplate);
        job.put("setting", setting);
        job.put("content", content);

        String json = JSON.toJSONString(new JSONObject().fluentPut("job", job), JSONWriter.Feature.PrettyFormat);
        if (outputFile == null) {
            System.out.println(json);
            System.out.flush();
        }
        else {
            writeFile(Path.of(outputFile), json);
        }
    }

    private void writeFile(Path path, String content)
    {
        try {
            if (Files.exists(path) && !overwrite) {
                System.err.println("Error: output file already exists: " + path + " (use --overwrite to replace)");
                return;
            }
            Files.writeString(path, content, StandardCharsets.UTF_8);
            // the file contains credentials, so restrict access to the owner
            if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                Set<PosixFilePermission> perms = Set.of(
                        PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
                Files.setPosixFilePermissions(path, perms);
            }
            System.err.println("Job written to " + path);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to write output file " + path, e);
        }
    }

    private JSONObject readTemplate(Path addaxHome, String type, String plugin)
    {
        Path template = addaxHome.resolve("plugin").resolve(type).resolve(plugin).resolve("plugin_job_template.json");
        if (!Files.exists(template)) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Plugin " + plugin + " is not installed (missing " + template + ")");
        }
        try {
            return JSON.parseObject(Files.readString(template, StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR, "Failed to read template " + template, e);
        }
    }

    private List<String> validateColumns(List<String> sourceNames, List<String> requested)
    {
        List<String> lower = sourceNames.stream().map(s -> s.toLowerCase(Locale.ROOT)).toList();
        for (String col : requested) {
            if (!lower.contains(col.toLowerCase(Locale.ROOT))) {
                System.err.println("Warning: column '" + col + "' not found in source table");
            }
        }
        return requested;
    }

    private void warnMissingColumns(List<String> selected, Map<String, String> targetTypes)
    {
        for (String column : selected) {
            if (!targetTypes.containsKey(column.toLowerCase(Locale.ROOT))) {
                System.err.println("Warning: column '" + column + "' does not exist in the target table");
            }
        }
    }

    private void warnTypeMismatches(List<SchemaProbe.ColumnInfo> sourceColumns, Map<String, String> targetTypes)
    {
        for (SchemaProbe.ColumnInfo column : sourceColumns) {
            String targetType = targetTypes.get(column.name.toLowerCase(Locale.ROOT));
            if (targetType != null && !targetType.equalsIgnoreCase(column.typeName)) {
                System.err.println("Warning: type mismatch for column '" + column.name
                        + "': source " + column.typeName + " -> target " + targetType);
            }
        }
    }

    private void suggestSimilar(List<String> tables, String wanted)
    {
        String lower = wanted.toLowerCase(Locale.ROOT);
        List<String> similar = tables.stream()
                .filter(t -> t.toLowerCase(Locale.ROOT).contains(lower) || lower.contains(t.toLowerCase(Locale.ROOT)))
                .limit(5)
                .toList();
        if (!similar.isEmpty()) {
            System.err.println("Did you mean: " + String.join(", ", similar));
        }
    }

    private void listPlugins()
    {
        Path addaxHome = Path.of(System.getProperty("addax.home", "."));
        System.out.println("Reader Plugins:");
        listDir(addaxHome.resolve("plugin/reader"));
        System.out.println();
        System.out.println("Writer Plugins:");
        listDir(addaxHome.resolve("plugin/writer"));
    }

    private void listDir(Path dir)
    {
        File[] entries = dir.toFile().listFiles();
        if (entries == null) {
            return;
        }
        Arrays.stream(entries).filter(File::isDirectory)
                .map(File::getName)
                .sorted()
                .forEach(name -> System.out.println("  " + name));
    }

    private String sourcePassword()
    {
        if (sourcePasswordEnv != null) {
            String fromEnv = System.getenv(sourcePasswordEnv);
            if (fromEnv == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Environment variable " + sourcePasswordEnv + " is not set");
            }
            return fromEnv;
        }
        return null;
    }

    /** Encrypts the password in the ${enc:...} form so no plaintext lands in the job file. */
    private String encrypt(String password)
    {
        if (password.isEmpty()) {
            return password;
        }
        return ENC_PASSWORD_PREFIX + EncryptUtil.encrypt(password) + "}";
    }

    private String requireValue(String[] args, int index, String option)
    {
        if (index >= args.length || args[index].startsWith("-")) {
            throw new IllegalArgumentException("Missing value for " + option);
        }
        return args[index];
    }

    private int parseInt(String[] args, int index)
    {
        try {
            return Integer.parseInt(requireValue(args, index, "--channel"));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid --channel value: " + args[index]);
        }
    }
}
