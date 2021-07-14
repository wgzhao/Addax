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

package com.wgzhao.addax.plugin.reader.dbffilereader;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.ColumnEntry;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.ColumnUtil;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.common.util.RecordUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by zhongtian.hu on 19-8-8.
 */
public class DbfFileReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> path = null;

        private List<String> sourceFiles;

        private Map<String, Pattern> pattern;

        private Map<String, Boolean> isRegexPath;

        private boolean isBlank(Object object)
        {
            if (null == object) {
                return true;
            }
            if ((object instanceof String)) {
                return "".equals(((String) object).trim());
            }
            return false;
        }

        @Override
        public void init()
        {
            this.originConfig = this.getPluginJobConf();
            this.pattern = new HashMap<>();
            this.isRegexPath = new HashMap<>();
            this.validateParameter();
        }

        private void validateParameter()
        {
            // Compatible with the old version, path is a string before
            String pathInString = this.originConfig.getNecessaryValue(Key.PATH, DbfFileReaderErrorCode.REQUIRED_VALUE);
            if (isBlank(pathInString)) {
                throw AddaxException.asAddaxException(
                        DbfFileReaderErrorCode.REQUIRED_VALUE,
                        "您需要指定待读取的源目录或文件");
            }
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            DbfFileReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定待读取的源目录或文件");
                }
            }

            String encoding = this.originConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            if (isBlank(encoding)) {
                this.originConfig.set(Key.ENCODING, Constant.DEFAULT_ENCODING);
            }
            else {
                try {
                    encoding = encoding.trim();
                    this.originConfig.set(Key.ENCODING, encoding);
                    Charsets.toCharset(encoding);
                }
                catch (UnsupportedCharsetException uce) {
                    throw AddaxException.asAddaxException(
                            DbfFileReaderErrorCode.ILLEGAL_VALUE,
                            String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(
                            DbfFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("编码配置异常, 请联系我们: %s", e.getMessage()),
                            e);
                }
            }
            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.originConfig.getListConfiguration(Key.COLUMN);
            if (null != column && 1 == column.size() && ("\"*\"".equals(column.get(0).toString())
                    || "'*'".equals(column.get(0).toString()))) {
                originConfig.set(Key.COLUMN, new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, may have format
                List<Configuration> columns = this.originConfig.getListConfiguration(Key.COLUMN);

                if (null == columns || columns.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            DbfFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.TYPE, DbfFileReaderErrorCode.REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(Key.INDEX);
                    String columnValue = eachColumnConf.getString(Key.VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(
                                DbfFileReaderErrorCode.NO_INDEX_VALUE,
                                "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(
                                DbfFileReaderErrorCode.MIXED_INDEX_VALUE,
                                "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.debug("prepare() begin...");
            // warn:make sure this regex string
            // warn:no need trim
            for (String eachPath : this.path) {
                String regexString = eachPath.replace("*", ".*").replace("?", ".?");
                Pattern pattern = Pattern.compile(regexString);
                this.pattern.put(eachPath, pattern);
            }
            this.sourceFiles = this.buildSourceTargets();

            LOG.info("您即将读取的文件数为: [{}]", this.sourceFiles.size());
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber)
        {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();

            // warn:每个slice拖且仅拖一个文件,
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(
                        DbfFileReaderErrorCode.EMPTY_DIR_EXCEPTION, String
                                .format("未能找到待读取的文件,请确认您的配置项path: %s",
                                        this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(this.sourceFiles, splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Key.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        // validate the path, path must be a absolute path
        private List<String> buildSourceTargets()
        {
            // for each path
            Set<String> toBeReadFiles = new HashSet<>();
            for (String eachPath : this.path) {
                LOG.info("parse path {}", eachPath);
                int endMark;
                for (endMark = 0; endMark < eachPath.length(); endMark++) {
                    if ('*' == eachPath.charAt(endMark) || '?' == eachPath.charAt(endMark)) {
                        this.isRegexPath.put(eachPath, true);
                        break;
                    }
                }

                String parentDirectory;
                if (!this.isRegexPath.isEmpty() && this.isRegexPath.containsKey(eachPath)) {
                    int lastDirSeparator = eachPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
                    parentDirectory = eachPath.substring(0, lastDirSeparator + 1);
                }
                else {
                    this.isRegexPath.put(eachPath, false);
                    parentDirectory = eachPath;
                }
                this.buildSourceTargetsEachPath(eachPath, parentDirectory, toBeReadFiles);
            }
            return Arrays.asList(toBeReadFiles.toArray(new String[0]));
        }

        private void buildSourceTargetsEachPath(String regexPath,
                String parentDirectory, Set<String> toBeReadFiles)
        {
            // 检测目录是否存在，错误情况更明确
            try {
                File dir = new File(parentDirectory);
                boolean isExists = dir.exists();
                if (!isExists) {
                    String message = String.format("您设定的目录不存在 : [%s]", parentDirectory);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(DbfFileReaderErrorCode.FILE_NOT_EXISTS, message);
                }
            }
            catch (SecurityException se) {
                String message = String.format("您没有权限查看目录 : [%s]", parentDirectory);
                LOG.error(message);
                throw AddaxException.asAddaxException(DbfFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
            }

            directoryRover(regexPath, parentDirectory, toBeReadFiles);
        }

        private void directoryRover(String regexPath, String parentDirectory,
                Set<String> toBeReadFiles)
        {
            File directory = new File(parentDirectory);
            // is a normal file
            if (!directory.isDirectory()) {
                if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
                    toBeReadFiles.add(parentDirectory);
                    LOG.info("add file [{}] as a candidate to be read.", parentDirectory);
                }
            }
            else {
                // 是目录
                try {
                    // warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
                    File[] files = directory.listFiles();
                    if (null != files) {
                        for (File subFileNames : files) {
                            directoryRover(regexPath, subFileNames.getAbsolutePath(), toBeReadFiles);
                        }
                    }
                    else {
                        // warn: 对于没有权限的文件，是直接throw DataXException
                        String message = String.format("您没有权限查看目录 : [%s]", directory);
                        LOG.error(message);
                        throw AddaxException.asAddaxException(DbfFileReaderErrorCode.SECURITY_NOT_ENOUGH, message);
                    }
                }
                catch (SecurityException e) {
                    String message = String.format("您没有权限查看目录 : [%s]", directory);
                    LOG.error(message);
                    throw AddaxException.asAddaxException(DbfFileReaderErrorCode.SECURITY_NOT_ENOUGH, message, e);
                }
            }
        }

        // 正则过滤
        private boolean isTargetFile(String regexPath, String absoluteFilePath)
        {
            if (Boolean.TRUE.equals(this.isRegexPath.get(regexPath))) {
                return this.pattern.get(regexPath).matcher(absoluteFilePath).matches();
            }
            else {
                return true;
            }
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
                int adviceNumber)
        {
            List<List<T>> splitedList = new ArrayList<>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }
    }

    public static class Task
            extends Reader.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        @Override
        public void init()
        {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceFiles = this.readerSliceConfig.getList(Key.SOURCE_FILES, String.class);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void post()
        {
            //
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            LOG.debug("begin reading dbf files...");
            String encode = readerSliceConfig.getString(Key.ENCODING);
            String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT);
            List<ColumnEntry> column = ColumnUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
            if (column == null || column.isEmpty()) {
                // get column description from dbf file
                column = getColumnInfo(this.sourceFiles.get(0), encode);
            }
            if (column == null) {
                throw AddaxException.asAddaxException(
                        DbfFileReaderErrorCode.RUNTIME_EXCEPTION,
                        "无法从指定的DBF文件(" + this.sourceFiles.get(0) + ")获取字段信息"
                );
            }
            int colNum = column.size();
            DBFRow row;
            for (String fileName : this.sourceFiles) {
                LOG.info("begin reading file : [{}]", fileName);
                try (DBFReader reader = new DBFReader(new FileInputStream(fileName), Charset.forName(encode))) {
                    while ((row = reader.nextRow()) != null) {
                        String[] sourceLine = new String[colNum];
                        for (int i = 0; i < colNum; i++) {
                            // constant value ?
                            if (column.get(i) != null && column.get(i).getValue() != null) {
                                sourceLine[i] = column.get(i).getValue();
                            } else if (row.getString(i) != null && "date".equalsIgnoreCase(column.get(i).getType())) {
                                // DBase's date type does not include time part
                                sourceLine[i] = new SimpleDateFormat("yyyy-MM-dd").format(row.getDate(i));
                            }
                            else {
                                sourceLine[i] = row.getString(i);
                            }
                        }
                        RecordUtil.transportOneRecord(recordSender, column, sourceLine, nullFormat, this.getTaskPluginCollector());
                    }
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            LOG.debug("end reading dbf files...");
        }

        /**
         * get column description from dbf file
         *
         * @param fpath dbf file path
         * @param encoding the dbf file encoding
         * @return list of column entry
         */
        private List<ColumnEntry> getColumnInfo(String fpath, String encoding)
        {
            List<ColumnEntry> column = new ArrayList<>();
            DBFDataType type;
            try (DBFReader reader = new DBFReader(new FileInputStream(fpath), Charset.forName(encoding))) {
                for (int i = 0; i < reader.getFieldCount(); i++) {
                    ColumnEntry columnEntry = new ColumnEntry();
                    columnEntry.setIndex(i);
                    type = reader.getField(i).getType();
                    switch (type) {
                        case DATE:
                            columnEntry.setType("date");
                            break;
                        case FLOATING_POINT:
                        case NUMERIC:
                        case DOUBLE:
                            columnEntry.setType("double");
                            break;
                        case LOGICAL:
                            columnEntry.setType("boolean");
                            break;
                        case MEMO:
                        case BINARY:
                        case BLOB:
                            columnEntry.setType("bytes");
                            break;
                        case CURRENCY:
                            columnEntry.setType("decimal");
                            break;
                        case LONG:
                        case AUTOINCREMENT:
                            columnEntry.setType("long");
                            break;
                        case TIMESTAMP:
                        case TIMESTAMP_DBASE7:
                            columnEntry.setType("timestamp");
                            break;
                        default:
                            columnEntry.setType("string");
                            break;
                    }
                    column.add(columnEntry);
                }
                return column;
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
