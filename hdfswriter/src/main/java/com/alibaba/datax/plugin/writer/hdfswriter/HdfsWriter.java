package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.Constant;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();

            //创建textfile存储
            hdfsHelper = new HdfsHelper();

            hdfsHelper.getFileSystem(defaultFS, this.writerSliceConfig);
        }

        private void validateParameter()
        {
            this.defaultFS = this.writerSliceConfig.getNecessaryValue(Key.DEFAULT_FS, HdfsWriterErrorCode.REQUIRED_VALUE);
            //fileType check
            String fileType = this.writerSliceConfig.getNecessaryValue(Key.FILE_TYPE, HdfsWriterErrorCode.REQUIRED_VALUE).toUpperCase();
            if (!Key.SUPPORT_FORMAT.contains(fileType)) {
                String message = String.format("[%s] 文件格式不支持， HdfsWriter插件目前仅支持 %s, ", fileType, Key.SUPPORT_FORMAT);
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            //path
            this.path = this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("请检查参数path:[%s],需要配置为绝对路径", path);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            if (path.contains("*") || path.contains("?")) {
                String message = String.format("请检查参数path:[%s],不能包含*,?等特殊字符", path);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            //fileName
            this.fileName = this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
            //columns check
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            if (null == columns || columns.isEmpty()) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.REQUIRED_VALUE, "您需要指定 columns");
            }
            else {
                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(Key.NAME, HdfsWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    eachColumnConf.getNecessaryValue(Key.TYPE, HdfsWriterErrorCode.COLUMN_REQUIRED_VALUE);
                }
            }
            //writeMode check
            this.writeMode = this.writerSliceConfig.getNecessaryValue(Key.WRITE_MODE, HdfsWriterErrorCode.REQUIRED_VALUE);
            writeMode = writeMode.toLowerCase().trim();
            Set<String> supportedWriteModes = new HashSet<>(Arrays.asList("append", "nonconflict", "overwrite"));
            if (!supportedWriteModes.contains(writeMode)) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅支持append, nonConflict,overwrite三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                writeMode));
            }
            this.writerSliceConfig.set(Key.WRITE_MODE, writeMode);
            //fieldDelimiter check
            String fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER, null);
            if (null == fieldDelimiter) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.REQUIRED_VALUE,
                        String.format("您提供配置文件有误，[%s]是必填参数.", Key.FIELD_DELIMITER));
            }
            else if (1 != fieldDelimiter.length()) {
                // warn: if have, length must be one
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", fieldDelimiter));
            }
            //compress check
            this.compress = this.writerSliceConfig.getString(Key.COMPRESS, "NONE").toUpperCase().trim();
            if ("ORC".equals(fileType)) {
                try {
                    CompressionKind.valueOf(compress.toUpperCase().trim());
                }
                catch (IllegalArgumentException e) {
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
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
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("目前PARQUET 格式仅支持 %s 压缩, 不支持您配置的 compress 模式 : [%s]",
                                    Arrays.toString(CompressionCodecName.values()), compress));
                }
            }
            if ("TEXT".equals(fileType)) {
                // SNAPPY需要动态库的支持，暂时放弃
                Set<String> textCompress = new HashSet<>(Arrays.asList("NONE", "GZIP", "BZIP2"));
                if (!textCompress.contains(compress.toUpperCase().trim())) {
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
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
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("不支持您配置的编码格式:[%s]", encoding), e);
            }
        }

        @Override
        public void prepare()
        {
            //若路径已经存在，检查path是否是目录
            if (hdfsHelper.isPathexists(path)) {
                if (!hdfsHelper.isPathDir(path)) {
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                    path));
                }

                //根据writeMode对目录下文件进行处理
                Path[] existFilePaths = hdfsHelper.hdfsDirList(path, null);

                boolean isExistFile = false;
                if (existFilePaths.length > 0) {
                    isExistFile = true;
                }
                if ("append".equalsIgnoreCase(writeMode)) {
                    LOG.info("由于您配置了writeMode append, 写入前不做清理工作, [{}] 目录下写入相应文件名前缀 [{}] 的文件",
                            path, fileName);
                }
                else if ("nonconflict".equalsIgnoreCase(writeMode) && isExistFile) {
                    LOG.info("由于您配置了writeMode nonConflict, 开始检查 [{}] 下面的内容", path);
                    List<String> allFiles = new ArrayList<>();
                    for (Path eachFile : existFilePaths) {
                        allFiles.add(eachFile.toString());
                    }
                    LOG.error("冲突文件列表为: [{}]", String.join(",", allFiles));
                    throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                            String.format("由于您配置了writeMode nonConflict,但您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
                }
            }
            else {
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的path: [%s] 不存在, 请先在hive端创建对应的数据库和表.", path));
            }
        }

        @Override
        public void post()
        {
            Path[] existFilePaths = hdfsHelper.hdfsDirList(path, null);
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
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String filePrefix = fileName;

            Set<String> allFiles = new HashSet<>();

            //获取该路径下的所有已有文件列表
            if (hdfsHelper.isPathexists(path)) {
                allFiles.addAll(Arrays.asList(hdfsHelper.hdfsDirList(path)));
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
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig.clone();
                String fullFileName;
                String endFullFileName;

                fileSuffix = UUID.randomUUID().toString().replace('-', '_');

                fullFileName = String.format("%s%s%s__%s", defaultFS, storePath, filePrefix, fileSuffix);
                endFullFileName = String.format("%s%s%s__%s", defaultFS, endStorePath, filePrefix, fileSuffix);

                while (allFiles.contains(endFullFileName)) {
                    fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                    fullFileName = String.format("%s%s%s__%s", defaultFS, storePath, filePrefix, fileSuffix);
                    endFullFileName = String.format("%s%s%s__%s", defaultFS, endStorePath, filePrefix, fileSuffix);
                }
                allFiles.add(endFullFileName);

                // 只有文本格式写入时，才会自动加上文件后缀，其他格式即便指定同样的压缩格式，也不会有后缀，因此这里需要加上判断
                if ("TEXT".equalsIgnoreCase(writerSliceConfig.getNecessaryValue(Key.FILE_TYPE, HdfsWriterErrorCode.REQUIRED_VALUE))) {
                    //设置临时文件全路径和最终文件全路径
                    if ("GZIP".equalsIgnoreCase(this.compress)) {
                        this.tmpFiles.add(fullFileName + ".gz");
                        this.endFiles.add(endFullFileName + ".gz");
                    }
                    else if ("BZIP2".equalsIgnoreCase(compress)) {
                        this.tmpFiles.add(fullFileName + ".bz2");
                        this.endFiles.add(endFullFileName + ".bz2");
                    }
                    else if ("ZLIB".equals(compress)) {
                        this.tmpFiles.add(fullFileName + ".deflate");
                        this.endFiles.add(endFullFileName + ".deflate");
                    }
                    else {
                        this.tmpFiles.add(fullFileName);
                        this.endFiles.add(endFullFileName);
                    }
                } else {
                    this.tmpFiles.add(fullFileName);
                    this.endFiles.add(endFullFileName);
                }

                splitedTaskConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                fullFileName);

                LOG.info("splited write file name:[{}]", fullFileName);

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

        private String buildFilePath()
        {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = this.path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = this.path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
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
            //得当的已经是绝对路径，eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
            this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);

            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, writerSliceConfig);
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
                hdfsHelper.textFileStartWrite(lineReceiver, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }
            else if ("ORC".equals(fileType)) {
                //写ORC FILE
                hdfsHelper.orcFileStartWrite(lineReceiver, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }
            else if ("PARQUET".equals(fileType)) {
                //写Parquet FILE
                hdfsHelper.parquetFileStartWrite(lineReceiver, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
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
