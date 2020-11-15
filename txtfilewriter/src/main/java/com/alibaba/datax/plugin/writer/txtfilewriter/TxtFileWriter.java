package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class TxtFileWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            String dateFormatOld = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FORMAT);
            String dateFormatNew = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT);
            if (null == dateFormatNew) {
                this.writerSliceConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT,
                                dateFormatOld);
            }
            if (null != dateFormatOld) {
                LOG.warn("您使用format配置日期格式化, 这是不推荐的行为, 请优先使用dateFormat配置项, 两项同时存在则使用dateFormat.");
            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
        }

        private void validateParameter()
        {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            TxtFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    TxtFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            }
            catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void prepare()
        {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);

            File dir = new File(path);
            //path is exists or not ?
            if (!dir.exists()) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.PATH_NOT_VALID, String.format("您配置的路径 [%s] 不存在", path)
                );
            }
            // path is directory or not ?
            if (!dir.isDirectory()) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.PAHT_NOT_DIR, String.format("您配置的路径 [%s] 不是文件夹", path)
                );
            }
            // path can writer it ?
            if (!dir.canWrite()) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.WRITE_FILE_ERROR, String.format("您配置的路径 [%s] 没有写入权限", path)
                );
            }
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info("由于您配置了writeMode truncate, 开始清理 [{}] 下面以 [{}] 开头的内容", path, fileName);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    FilenameFilter filter = new PrefixFileFilter(fileName);
                    File[] filesWithFileNamePrefix = dir.listFiles(filter);
                    assert filesWithFileNamePrefix != null;
                    for (File eachFile : filesWithFileNamePrefix) {
                        LOG.info("delete file [{}].", eachFile.getName());
                        FileUtils.forceDelete(eachFile);
                    }
                }
                catch (NullPointerException npe) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.WRITE_FILE_ERROR,
                                    String.format("您配置的目录清空时出现空指针异常 : [%s]",
                                            path), npe);
                }
                catch (IllegalArgumentException iae) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 : [%s]", path));
                }
                catch (IOException e) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.WRITE_FILE_ERROR,
                            String.format("无法清空目录 : [%s]", path), e);
                }
            }
            else if ("append".equals(writeMode)) {
                LOG.info("由于您配置了writeMode append, 写入前不做清理工作, [{}] 目录下写入相应文件名前缀 [{}] 的文件", path, fileName);
            }
            else if ("nonConflict".equals(writeMode)) {
                LOG.info("由于您配置了writeMode nonConflict, 开始检查 [{}] 下面的内容", path);
                // warn: check two times about exists, mkdirs
                try {
                    if (dir.exists()) {
                        // fileName is not null
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        assert filesWithFileNamePrefix != null;
                        if (filesWithFileNamePrefix.length > 0) {
                            List<String> allFiles = new ArrayList<>();
                            for (File eachFile : filesWithFileNamePrefix) {
                                allFiles.add(eachFile.getName());
                            }
                            LOG.error("冲突文件列表为: [{}]", StringUtils.join(allFiles, ","));
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                                    path));
                        }
                    }
                    else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                            String.format(
                                                    "您指定的文件路径 : [%s] 创建失败.",
                                                    path));
                        }
                    }
                }
                catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                }
            }
            else {
                throw DataXException
                        .asDataXException(
                                TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
            }
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
        public List<Configuration> split(int mandatoryNumber)
        {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<>();
            String filePrefix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);

            Set<String> allFiles;
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles = new HashSet<>(Arrays.asList(Objects.requireNonNull(dir.list())));
            }
            catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
            }

            String fileSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullFileName;
                fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
                while (allFiles.contains(fullFileName)) {
                    fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                    fullFileName = String.format("%s__%s", filePrefix,
                            fileSuffix);
                }
                allFiles.add(fullFileName);

                splitedTaskConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                fullFileName);

                LOG.info("splited write file name:[{}]", fullFileName);

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }
    }

    public static class Task
            extends Writer.Task
    {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String fileName;

        @Override
        public void init()
        {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info("write to file : [{}]", fileFullPath);

            OutputStream outputStream = null;
            try {
                File newFile = new File(fileFullPath);
                boolean isSucess = newFile.createNewFile();
                assert isSucess;
                outputStream = new FileOutputStream(newFile);
                UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
                        outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            }
            catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            }
            catch (IOException ioe) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.WRITE_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), ioe);
            }
            finally {
                IOUtils.closeQuietly(outputStream, null);
            }
            LOG.info("end do write");
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
            return String.format("%s%s", this.path, this.fileName);
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
