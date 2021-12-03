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

package com.wgzhao.addax.plugin.writer.hdfswriter;

import com.wgzhao.addax.common.base.Constant;
import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.util.FileHelper;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private final HashSet<String> tmpFiles = new HashSet<>();//临时文件全路径
        private final HashSet<String> endFiles = new HashSet<>();//最终文件全路径
        private Configuration writerSliceConfig = null;
        private String defaultFS;
        private String path;
        private String fileName;
        private String writeMode;
        private String compress;
        private HdfsHelper hdfsHelper = null;

        public static final Set<String> SUPPORT_FORMAT = new HashSet<>(Arrays.asList("ORC", "PARQUET", "TEXT"));

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();

            hdfsHelper = new HdfsHelper();

            hdfsHelper.getFileSystem(defaultFS, this.writerSliceConfig);
        }

        private void validateParameter()
        {
            this.defaultFS = this.writerSliceConfig.getNecessaryValue(Key.DEFAULT_FS, HdfsWriterErrorCode.REQUIRED_VALUE);
            //fileType check
            String fileType = this.writerSliceConfig.getNecessaryValue(Key.FILE_TYPE, HdfsWriterErrorCode.REQUIRED_VALUE).toUpperCase();
            if (!SUPPORT_FORMAT.contains(fileType)) {
                String message = String.format("[%s] 文件格式不支持， HdfsWriter插件目前仅支持 %s, ", fileType, SUPPORT_FORMAT);
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            //path
            this.path = this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("请检查参数path:[%s],需要配置为绝对路径", path);
                LOG.error(message);
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            if (path.contains("*") || path.contains("?")) {
                String message = String.format("请检查参数path:[%s],不能包含*,?等特殊字符", path);
                LOG.error(message);
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            //fileName
            this.fileName = this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
            //columns check
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            if (null == columns || columns.isEmpty()) {
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.REQUIRED_VALUE, "您需要指定 columns");
            }
            else {
                boolean rewriteFlag = false;
                for (int i = 0; i < columns.size(); i++) {
                    Configuration eachColumnConf = columns.get(i);
                    eachColumnConf.getNecessaryValue(Key.NAME, HdfsWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    eachColumnConf.getNecessaryValue(Key.TYPE, HdfsWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    if (eachColumnConf.getString(Key.TYPE).toUpperCase().startsWith("DECIMAL")) {
                        String type = eachColumnConf.getString(Key.TYPE);
                        eachColumnConf.set(Key.TYPE, "decimal");
                        eachColumnConf.set(Key.PRECISION, getDecimalPrecision(type));
                        eachColumnConf.set(Key.SCALE, getDecimalScale(type));
                        columns.set(i, eachColumnConf);
                        rewriteFlag = true;
                    }
                }
                if (rewriteFlag) {
                    this.writerSliceConfig.set(Key.COLUMN, columns);
                }
            }
            //writeMode check
            this.writeMode = this.writerSliceConfig.getNecessaryValue(Key.WRITE_MODE, HdfsWriterErrorCode.REQUIRED_VALUE);
            if (!Constant.SUPPORTED_WRITE_MODE.contains(writeMode)) {
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持append, nonConflict,overwrite三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                writeMode));
            }
            this.writerSliceConfig.set(Key.WRITE_MODE, writeMode);
            //fieldDelimiter check
            String fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER, null);
            if (null == fieldDelimiter) {
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.REQUIRED_VALUE,
                        String.format("您提供配置文件有误，[%s]是必填参数.", Key.FIELD_DELIMITER));
            }
            else if (1 != fieldDelimiter.length()) {
                // warn: if it has, length must be one
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", fieldDelimiter));
            }
            //compress check
            this.compress = this.writerSliceConfig.getString(Key.COMPRESS, "NONE").toUpperCase().trim();
            if ("ORC".equals(fileType)) {
                try {
                    CompressionKind.valueOf(compress);
                }
                catch (IllegalArgumentException e) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("目前ORC 格式仅支持 %s 压缩，不支持您配置的 compress 模式 : [%s]",
                                    Arrays.toString(CompressionKind.values()), compress));
                }
            }
            if ("PARQUET".equals(fileType)) {
                // parquet 默认的非压缩标志是 UNCOMPRESSED ，而不是常见的 NONE，这里统一为 NONE
                if ("NONE".equals(compress)) {
                    compress = "UNCOMPRESSED";
                }
                try {
                    CompressionCodecName.fromConf(compress);
                }
                catch (Exception e) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("目前PARQUET 格式仅支持 %s 压缩, 不支持您配置的 compress 模式 : [%s]",
                                    Arrays.toString(CompressionCodecName.values()), compress));
                }
            }
            if ("TEXT".equals(fileType)) {
                // SNAPPY需要动态库的支持，暂时放弃
                Set<String> textCompress = new HashSet<>(Arrays.asList("NONE", "GZIP", "BZIP2"));
                if (!textCompress.contains(compress.toUpperCase().trim())) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("目前TEXT 格式仅支持 %s 压缩，不支持您配置的 compress 模式 : [%s]",
                                    textCompress, compress));
                }
            }

            //Kerberos check
            boolean haveKerberos = this.writerSliceConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
                this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsWriterErrorCode.REQUIRED_VALUE);
            }
            // encoding check
            String encoding = this.writerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            try {
                encoding = encoding.trim();
                this.writerSliceConfig.set(Key.ENCODING, encoding);
                Charsets.toCharset(encoding);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式:[%s]", encoding), e);
            }
        }

        @Override
        public void prepare()
        {
            //若路径已经存在，检查path是否是目录
            if (hdfsHelper.isPathexists(path)) {
                if (!hdfsHelper.isPathDir(path)) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                    path));
                }

                //根据writeMode对目录下文件进行处理
                Path[] existFilePaths = hdfsHelper.hdfsDirList(path);

                boolean isExistFile = existFilePaths.length > 0;
                if ("append".equals(writeMode)) {
                    LOG.info("由于您配置了writeMode append, 写入前不做清理工作, [{}] 目录下写入相应文件名前缀 [{}] 的文件",
                            path, fileName);
                }
                else if ("nonConflict".equals(writeMode) && isExistFile) {
                    LOG.info("由于您配置了writeMode nonConflict, 开始检查 [{}] 下面的内容", path);
                    List<String> allFiles = new ArrayList<>();
                    for (Path eachFile : existFilePaths) {
                        allFiles.add(eachFile.toString());
                    }
                    LOG.error("冲突文件列表为: [{}]", String.join(",", allFiles));
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("由于您配置了writeMode nonConflict,但您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
                }
            }
            else {
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的path: [%s] 不存在, 请先在hive端创建对应的数据库和表.", path));
            }
        }

        @Override
        public void post()
        {
            Path[] existFilePaths = hdfsHelper.hdfsDirList(path);
            if ("overwrite".equals(writeMode)) {
                hdfsHelper.deleteFiles(existFilePaths, false);
            }
            hdfsHelper.renameFile(this.tmpFiles, this.endFiles);
            // 删除临时目录
            hdfsHelper.deleteFiles(existFilePaths, true);
        }

        @Override
        public void destroy()
        {
            hdfsHelper.closeFileSystem();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            LOG.info("begin splitting ...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String filePrefix = fileName;

            Set<String> allFiles = new HashSet<>();

            //获取该路径下的所有已有文件列表
            if (hdfsHelper.isPathexists(path)) {
                for (Path p : hdfsHelper.hdfsDirList(path)) {
                    allFiles.add(p.toString());
                }
            }

            String fileSuffix;
            //临时存放路径
            String storePath = buildTmpFilePath(this.path);
            if (storePath != null && storePath.contains("/")) {
                //由于在window上调试获取的路径为转义字符代表的斜杠
                //故出现被认为是文件名称的一部分，使得获取父目录存在问题
                //最终影响到直接删除更高一层的目录，导致Hive数据出现问题。
                storePath = storePath.replace('\\', '/');
            }
            //最终存放路径
            String endStorePath = buildFilePath();
            if (endStorePath != null && endStorePath.contains("/")) {
                endStorePath = endStorePath.replace('\\', '/');
            }
            this.path = endStorePath;
            String suffix = FileHelper.getCompressFileSuffix(this.compress);
            String fileType = this.writerSliceConfig.getString(Key.FILE_TYPE, "txt").toLowerCase();
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name
                Configuration splitedTaskConfig = this.writerSliceConfig.clone();
                String fullFileName;
                String endFullFileName;

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
                // like 2021-12-03-14-33-29-237-6587fddb
                fileSuffix = dateFormat.format(new Date()) + "-" + RandomStringUtils.random(8,"0123456789abcdef");
                fullFileName = String.format("%s%s%s__%s.%s%s", defaultFS, storePath, filePrefix, fileSuffix, fileType, suffix);
                endFullFileName = String.format("%s%s%s__%s.%s%s", defaultFS, endStorePath, filePrefix, fileSuffix, fileType,suffix);

                while (allFiles.contains(endFullFileName)) {
                    fileSuffix = dateFormat.format(new Date()) + "-" + RandomStringUtils.random(8,"0123456789abcdef");
                    fullFileName = String.format("%s%s%s__%s.%s%s", defaultFS, storePath, filePrefix, fileSuffix, fileType, suffix);
                    endFullFileName = String.format("%s%s%s__%s.%s%s", defaultFS, endStorePath, filePrefix, fileSuffix, fileType, suffix);
                }
                allFiles.add(endFullFileName);
                this.tmpFiles.add(fullFileName);
                this.endFiles.add(endFullFileName);

                splitedTaskConfig.set(Key.FILE_NAME, fullFileName);

                LOG.info("split wrote file name:[{}]", fullFileName);

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end splitting.");
            return writerSplitConfigs;
        }

        private String buildFilePath()
        {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = this.path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = this.path.endsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return this.path;
        }

        /**
         * 创建临时目录
         *
         * @param userPath hdfs path
         * @return string
         */
        private String buildTmpFilePath(String userPath)
        {
            // 把临时文件直接放置到/tmp目录下，避免删除老文件，移动文件等打来的逻辑复杂化
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = userPath.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = userPath.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            String tmpSuffix;
            tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
            //临时目录
            String tmpFilePath;
            String pattern = "%s/.%s%s";
            if (!isEndWithSeparator) {
                tmpFilePath = String.format(pattern, userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
            }
            else {
                tmpFilePath = String.format(pattern, userPath.substring(0, userPath.length() - 1), tmpSuffix, IOUtils.DIR_SEPARATOR);
            }
            while (hdfsHelper.isPathexists(tmpFilePath)) {
                tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
                if (!isEndWithSeparator) {
                    tmpFilePath = String.format(pattern, userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
                }
                else {
                    tmpFilePath = String.format(pattern, userPath.substring(0, userPath.length() - 1), tmpSuffix, IOUtils.DIR_SEPARATOR);
                }
            }
            return tmpFilePath;
        }

        /**
         * get decimal type precision
         * if not specified, use DECIMAL_DEFAULT_PRECISION as default
         * example:
         * <pre>
         *  decimal -&gt; 38
         *  decimal(10) -&gt; 10
         *  </pre>
         *
         * @param type decimal type including precision and scale (if present)
         * @return decimal precision
         */
        private static int getDecimalPrecision(String type)
        {
            if (!type.contains("(")) {
                return Constant.DEFAULT_DECIMAL_PRECISION;
            }
            else {
                String regEx = "[^0-9]";
                Pattern p = Pattern.compile(regEx);
                Matcher m = p.matcher(type);
                return Integer.parseInt(m.replaceAll(" ").trim().split(" ")[0]);
            }
        }

        /**
         * get decimal type scale
         * if precision is not present, return DECIMAL_DEFAULT_SCALE
         * if precision is present and not specify scale, return 0
         * example:
         * <pre>
         *  decimal -&gt; 10
         *  decimal(8) -&gt; 0
         *  decimal(8,2) -&gt; 2
         *  </pre>
         *
         * @param type decimal type string, including precision and scale (if present)
         * @return decimal scale
         */
        private static int getDecimalScale(String type)
        {
            if (!type.contains("(")) {
                return Constant.DEFAULT_DECIMAL_SCALE;
            }
            if (!type.contains(",")) {
                return 0;
            }
            else {
                return Integer.parseInt(type.split(",")[1].replace(")", "").trim());
            }
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String fileType;
        private String fileName;

        private HdfsHelper hdfsHelper = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();

            String defaultFS = this.writerSliceConfig.getString(Key.DEFAULT_FS);
            this.fileType = this.writerSliceConfig.getString(Key.FILE_TYPE).toUpperCase();

            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, writerSliceConfig);
            //得当的已经是绝对路径，eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.snappy
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("write to file : [{}]", this.fileName);
            if ("TEXT".equals(fileType)) {
                //写TEXT FILE
                hdfsHelper.textFileStartWrite(lineReceiver, this.writerSliceConfig, this.fileName, this.getTaskPluginCollector());
            }
            else if ("ORC".equals(fileType)) {
                //写ORC FILE
                hdfsHelper.orcFileStartWrite(lineReceiver, this.writerSliceConfig, this.fileName, this.getTaskPluginCollector());
            }
            else if ("PARQUET".equals(fileType)) {
                //写Parquet FILE
                hdfsHelper.parquetFileStartWrite(lineReceiver, this.writerSliceConfig, this.fileName, this.getTaskPluginCollector());
            }

            LOG.info("end do write");
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
    }
}
