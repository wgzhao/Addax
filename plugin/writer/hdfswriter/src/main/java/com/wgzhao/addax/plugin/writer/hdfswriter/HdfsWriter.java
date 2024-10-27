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
import org.apache.commons.exec.DefaultExecutor;
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

import org.apache.commons.exec.CommandLine;

import static com.wgzhao.addax.common.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.REQUIRED_VALUE;
import static com.wgzhao.addax.common.spi.ErrorCode.RUNTIME_ERROR;

public class HdfsWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private static final String SKIP_TRASH = "skipTrash";

        // The temporary directory where the file(s) are written will be deleted after the write operation is complete.
        private String tmpStorePath;
        private Configuration writerSliceConfig = null;
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

            hdfsHelper.getFileSystem(this.writerSliceConfig);
        }

        private void validateParameter()
        {
            String defaultFS = this.writerSliceConfig.getNecessaryValue(Key.DEFAULT_FS, REQUIRED_VALUE);
            //fileType check
            String fileType = this.writerSliceConfig.getNecessaryValue(Key.FILE_TYPE, REQUIRED_VALUE).toUpperCase();
            if (!SUPPORT_FORMAT.contains(fileType)) {
                String message = String.format("The file format [%s] is supported yet,  the plugin currently only supports: [%s].", fileType, SUPPORT_FORMAT);
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, message);
            }
            //path
            this.path = this.writerSliceConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("The path [%s] must be configured as an absolute path.", path);
                LOG.error(message);
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, message);
            }
            if (path.contains("*") || path.contains("?")) {
                String message = String.format("The path [%s] cannot contain special characters like '*','?'.", path);
                LOG.error(message);
                throw AddaxException.asAddaxException(ILLEGAL_VALUE, message);
            }
            //fileName
            this.fileName = this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, REQUIRED_VALUE);
            //columns check
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            if (null == columns || columns.isEmpty()) {
                throw AddaxException.asAddaxException(REQUIRED_VALUE, "The item columns should be configured");
            }
            else {
                boolean rewriteFlag = false;
                for (int i = 0; i < columns.size(); i++) {
                    Configuration eachColumnConf = columns.get(i);
                    eachColumnConf.getNecessaryValue(Key.NAME, REQUIRED_VALUE);
                    eachColumnConf.getNecessaryValue(Key.TYPE, REQUIRED_VALUE);
                    if (eachColumnConf.getString(Key.TYPE).toUpperCase().startsWith("DECIMAL")) {
                        String type = eachColumnConf.getString(Key.TYPE);
                        eachColumnConf.set(Key.TYPE, "decimal");
                        eachColumnConf.set(Key.PRECISION, (Object) getDecimalPrecision(type));
                        eachColumnConf.set(Key.SCALE, (Object) getDecimalScale(type));
                        columns.set(i, eachColumnConf);
                        rewriteFlag = true;
                    }
                }
                if (rewriteFlag) {
                    this.writerSliceConfig.set(Key.COLUMN, columns);
                }
            }
            //writeMode check
            this.writeMode = this.writerSliceConfig.getNecessaryValue(Key.WRITE_MODE, REQUIRED_VALUE);
            if (!Constant.SUPPORTED_WRITE_MODE.contains(writeMode)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The item writeMode only supports append, noConflict and overwrite, [%s] is unsupported yet.",
                                writeMode));
            }
            if ("TEXT".equals(fileType)) {
                //fieldDelimiter check
                String fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER, null);
                if (StringUtils.isEmpty(fieldDelimiter)) {
                    throw AddaxException.asAddaxException(REQUIRED_VALUE,
                            String.format("The item [%s] should be configured and valid while write TEXT file.", Key.FIELD_DELIMITER));
                }

                if (1 != fieldDelimiter.length()) {
                    // warn: if it has, length must be one
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The field delimiter is only support single character, your configure: [%s]", fieldDelimiter));
                }
            }

            //compress check
            String compress = this.writerSliceConfig.getString(Key.COMPRESS, "NONE").toUpperCase().trim();
            if ("ORC".equals(fileType)) {
                try {
                    CompressionKind.valueOf(compress);
                }
                catch (IllegalArgumentException e) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The ORC format only supports [%s] compression. your configure [%s] is unsupported yet.",
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
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The PARQUET format only supports [%s] compression. your configure [%s] is unsupported yet.",
                                    Arrays.toString(CompressionCodecName.values()), compress));
                }
            }

            //Kerberos check
            boolean haveKerberos = this.writerSliceConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, REQUIRED_VALUE);
                this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, REQUIRED_VALUE);
            }
            // encoding check
            String encoding = this.writerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            try {
                encoding = encoding.trim();
                this.writerSliceConfig.set(Key.ENCODING, encoding);
                Charsets.toCharset(encoding);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The encoding [%s] is unsupported yet.", encoding), e);
            }

            // trash
            this.skipTrash = this.writerSliceConfig.getBool(SKIP_TRASH, false);
        }

        @Override
        public void prepare()
        {
            // check preShell item
            List<String> preShells = this.writerSliceConfig.getList("preShell", String.class);
            if (!preShells.isEmpty()) {
                for (String preShell : preShells) {
                    execShell(preShell);
                }
            }

            this.tmpStorePath = buildTmpFilePath(path);

            // Verify whether the path is a directory if it exists.
            if (hdfsHelper.isPathExists(path)) {
                if (!hdfsHelper.isPathDir(path)) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The item path you configured [%s] is exists ,but it is not directory", path));
                }

                Path[] existFilePaths = hdfsHelper.hdfsDirList(path);

                boolean isExistFile = existFilePaths.length > 0;
                if ("append".equals(writeMode)) {
                    LOG.info("The current write mode is set to 'append', no cleanup is performed before writing. " +
                            "Files with the prefix [{}] are written in the [{}] directory.", fileName, path);
                }
                else if ("nonConflict".equals(writeMode) && isExistFile) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The current writeMode is set to 'nonConflict', but the directory [%s] is not empty, it includes the sub-path(s): [%s]",
                                    path, String.join(",", Arrays.stream(existFilePaths).map(Path::getName).collect(Collectors.toSet()))));
                }
            }
            else {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The directory [%s]  does not exists. please create it first. ", path));
            }

            // validate the write permission
            if (!hdfsHelper.isPathWritable(path)) {
                throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                        String.format("The path [%s] is not writable or permission denied", path));
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

            //check postShell item
            List<String> postShells = this.writerSliceConfig.getList("postShell", String.class);
            if (!postShells.isEmpty()) {
                for (String postShell : postShells) {
                    execShell(postShell);
                }
            }
        }

        @Override
        public void destroy()
        {
            hdfsHelper.closeFileSystem();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            LOG.info("Begin splitting ...");

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

                // 如果文件已经存在，则重新生成文件名
                do {
                    tmpFullFileName = String.format("%s/%s_%s.%s", tmpStorePath, filePrefix, FileHelper.generateFileMiddleName(), fileType);
                    endFullFileName = String.format("%s/%s_%s.%s", path, filePrefix, FileHelper.generateFileMiddleName(), fileType);
                }
                while (allFiles.contains(endFullFileName));
                allFiles.add(endFullFileName);

                splitTaskConfig.set(Key.FILE_NAME, tmpFullFileName);

                LOG.info("The split wrote files :[{}]", tmpFullFileName);

                writerSplitConfigs.add(splitTaskConfig);
            }
            LOG.info("Finish splitting.");
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

        private static void execShell(String command)
        {
            CommandLine cmdLine = CommandLine.parse(command);
            DefaultExecutor executor = DefaultExecutor.builder().get();
            LOG.info("Running command: {}", command);
            try {
                int retCode = executor.execute(cmdLine);
                if (retCode != 0) {
                    throw AddaxException.asAddaxException(EXECUTE_FAIL, String.format("Command [%s] exited with code %d", command, retCode));
                }
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(RUNTIME_ERROR, e);
            }
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        @Override
        public void init()
        {

            this.writerSliceConfig = this.getPluginJobConf();
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            String fileType = this.writerSliceConfig.getString(Key.FILE_TYPE).toUpperCase();
            IHDFSWriter hdfsHelper = null;
            switch (fileType) {
                case "TEXT":
                    hdfsHelper = new TextWriter(this.writerSliceConfig);
                    break;
                case "ORC":
                    hdfsHelper = new OrcWriter(this.writerSliceConfig);
                    break;
                case "PARQUET":
                    hdfsHelper = new ParquetWriter(this.writerSliceConfig);
                    break;
                default:
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE,
                            String.format("The file format [%s] is supported yet,  the plugin currently only supports: [%s].", fileType, Job.SUPPORT_FORMAT));
            }
            //得当的已经是绝对路径，eg：/user/hive/warehouse/writer.db/text/test.snappy
            String fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
            LOG.info("Begin to write file : [{}]", fileName);
            hdfsHelper.write(lineReceiver, writerSliceConfig, fileName, getTaskPluginCollector());
            LOG.info("Finish write");
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
