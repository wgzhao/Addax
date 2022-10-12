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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HdfsWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private static final String SKIP_TRASH = "skipTrash";

        // 写入文件的临时目录，完成写入后，该目录需要删除
        private String tmpStorePath;
        private Configuration writerSliceConfig = null;
        private String defaultFS;
        private String path;
        private String fileName;
        private String writeMode;
        private HdfsHelper hdfsHelper = null;

        // option bypasses trash, if enabled, and immediately deletes
        private boolean skipTrash = false;

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
                        String.format("仅支持append, nonConflict, overwrite三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                writeMode));
            }
            if ("TEXT".equals(fileType)) {
                //fieldDelimiter check
                String fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER, null);
                if (StringUtils.isEmpty(fieldDelimiter)) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.REQUIRED_VALUE,
                            String.format("写TEXT格式文件，必须提供有效的[%s] 参数.", Key.FIELD_DELIMITER));
                }

                if (1 != fieldDelimiter.length()) {
                    // warn: if it has, length must be one
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", fieldDelimiter));
                }
            }

            //compress check
            String compress = this.writerSliceConfig.getString(Key.COMPRESS, "NONE").toUpperCase().trim();
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

            // trash
            this.skipTrash = this.writerSliceConfig.getBool(SKIP_TRASH, false);
        }

        @Override
        public void prepare()
        {
            //临时存放路径
            this.tmpStorePath = buildTmpFilePath(path);

            //若路径已经存在，检查path是否是目录
            if (hdfsHelper.isPathExists(path)) {
                if (!hdfsHelper.isPathDir(path)) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.", path));
                }

                //根据writeMode对目录下文件进行处理
                // 写入之前，当前目录下已有的文件，根据writeMode判断是否覆盖
                Path[] existFilePaths = hdfsHelper.hdfsDirList(path);

                boolean isExistFile = existFilePaths.length > 0;
                if ("append".equals(writeMode)) {
                    LOG.info("由于您配置了writeMode = append, 写入前不做清理工作, [{}] 目录下写入相应文件名前缀 [{}] 的文件", path, fileName);
                }
                else if ("nonConflict".equals(writeMode) && isExistFile) {
                    throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("由于您配置了writeMode= nonConflict,但您配置的 path: [%s] 目录不为空, 下面存在其他文件或文件夹: %s",
                                    path, String.join(",", Arrays.stream(existFilePaths).map(Path::getName).collect(Collectors.toSet()))));
                }
            }
            else {
                throw AddaxException.asAddaxException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的path: [%s] 不存在, 请先创建目录.", path));
            }
        }

        @Override
        public void post()
        {
            if ("overwrite".equals(writeMode)) {
                hdfsHelper.deleteFilesFromDir(new Path(path), this.skipTrash);
            }

            hdfsHelper.moveFilesToDest(new Path(this.tmpStorePath), new Path(this.path));

            // 删除临时目录
            hdfsHelper.deleteDir(new Path(tmpStorePath));
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

            //获取该路径下的所有已有文件列表
            Set<String> allFiles = Arrays.stream(hdfsHelper.hdfsDirList(path)).map(Path::toString).collect(Collectors.toSet());

            String fileType = this.writerSliceConfig.getString(Key.FILE_TYPE, "txt").toLowerCase();
            String tmpFullFileName;
            String endFullFileName;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name
                Configuration splitTaskConfig = this.writerSliceConfig.clone();
                tmpFullFileName = String.format("%s/%s_%s.%s", tmpStorePath, filePrefix, FileHelper.generateFileMiddleName(), fileType);
                endFullFileName = String.format("%s/%s_%s.%s", path, filePrefix, FileHelper.generateFileMiddleName(), fileType);

                // 如果文件已经存在，则重新生成文件名
                while (allFiles.contains(endFullFileName)) {
                    tmpFullFileName = String.format("%s/%s_%s.%s", tmpStorePath, filePrefix, FileHelper.generateFileMiddleName(), fileType);
                    endFullFileName = String.format("%s/%s_%s.%s", path, filePrefix, FileHelper.generateFileMiddleName(), fileType);
                }
                allFiles.add(endFullFileName);

                splitTaskConfig.set(Key.FILE_NAME, tmpFullFileName);

                LOG.info("split wrote file name:[{}]", tmpFullFileName);

                writerSplitConfigs.add(splitTaskConfig);
            }
            LOG.info("end splitting.");
            return writerSplitConfigs;
        }

        /**
         * 创建临时目录
         * 在给定目录的下，创建一个已点开头，uuid为名字的文件夹，用于临时存储写入的文件
         *
         * @param userPath hdfs path
         * @return temporary path
         */
        private String buildTmpFilePath(String userPath)
        {
            String tmpDir;
            String tmpFilePath;

            while (true) {
                tmpDir = "." + UUID.randomUUID().toString().replace('-', '_');
                tmpFilePath = Paths.get(userPath, tmpDir).toString();
                if (!hdfsHelper.isPathExists(tmpFilePath)) {
                    return tmpFilePath;
                }
            }
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
                return Constant.DEFAULT_DECIMAL_MAX_PRECISION;
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
                return Constant.DEFAULT_DECIMAL_MAX_SCALE;
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
            //得当的已经是绝对路径，eg：/user/hive/warehouse/writer.db/text/test.snappy
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
                hdfsHelper.textFileStartWrite(lineReceiver, writerSliceConfig, fileName, getTaskPluginCollector());
            }
            else if ("ORC".equals(fileType)) {
                //写ORC FILE
                hdfsHelper.orcFileStartWrite(lineReceiver, writerSliceConfig, fileName, getTaskPluginCollector());
            }
            else if ("PARQUET".equals(fileType)) {
                //写Parquet FILE
                hdfsHelper.parquetFileStartWrite(lineReceiver, writerSliceConfig, fileName, getTaskPluginCollector());
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
