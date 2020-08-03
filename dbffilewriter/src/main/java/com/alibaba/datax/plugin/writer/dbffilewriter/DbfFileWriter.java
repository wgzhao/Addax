package com.alibaba.datax.plugin.writer.dbffilewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;

import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFUtils;
import com.linuxense.javadbf.DBFWriter;
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
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class DbfFileWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        @Override
        public void init() {
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

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            DbfFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    DbfFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        DbfFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void prepare() {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                        path, fileName));
                File dir = new File(path);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    if (dir.exists()) {
                        // warn:不要使用FileUtils.deleteQuietly(dir);
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        for (File eachFile : filesWithFileNamePrefix) {
                            LOG.info(String.format("delete file [%s].",
                                    eachFile.getName()));
                            FileUtils.forceDelete(eachFile);
                        }
                        // FileUtils.cleanDirectory(dir);
                    }
                } catch (NullPointerException npe) {
                    throw DataXException
                            .asDataXException(
                                    DbfFileWriterErrorCode.Write_FILE_ERROR,
                                    String.format("您配置的目录清空时出现空指针异常 : [%s]",
                                            path), npe);
                } catch (IllegalArgumentException iae) {
                    throw DataXException.asDataXException(
                            DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 : [%s]", path));
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                } catch (IOException e) {
                    throw DataXException.asDataXException(
                            DbfFileWriterErrorCode.Write_FILE_ERROR,
                            String.format("无法清空目录 : [%s]", path), e);
                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                path, fileName));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                // warn: check two times about exists, mkdirs
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw DataXException
                                    .asDataXException(
                                            DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                                    path));
                        }
                        // fileName is not null
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        if (filesWithFileNamePrefix.length > 0) {
                            List<String> allFiles = new ArrayList<String>();
                            for (File eachFile : filesWithFileNamePrefix) {
                                allFiles.add(eachFile.getName());
                            }
                            LOG.error(String.format("冲突文件列表为: [%s]",
                                    StringUtils.join(allFiles, ",")));
                            throw DataXException
                                    .asDataXException(
                                            DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                                    path));
                        }
                    } else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw DataXException
                                    .asDataXException(
                                            DbfFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                            String.format(
                                                    "您指定的文件路径 : [%s] 创建失败.",
                                                    path));
                        }
                    }
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                }
            } else {
                throw DataXException
                        .asDataXException(
                                DbfFileWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String filePrefix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);

            Set<String> allFiles = new HashSet<String>();
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles.addAll(Arrays.asList(dir.list()));
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
            }

            String fileSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullFileName = null;
                if (mandatoryNumber>1){
                    fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                    fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
                    while (allFiles.contains(fullFileName)) {
                        fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                        fullFileName = String.format("%s__%s", filePrefix,
                                fileSuffix);
                    }

                }else {
                    fullFileName=filePrefix;
                }
                allFiles.add(fullFileName);

                splitedTaskConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                fullFileName);

                LOG.info(String.format("splited write file name:[%s]",
                        fullFileName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String fileName;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration("column");
            DBFWriter writer = null;
            FileOutputStream fos = null;
            try {
                File f = new File(fileFullPath);
                f.createNewFile();
                fos = new FileOutputStream(f);
                writer = new DBFWriter(fos, Charset.forName("GBK"));
                DBFField[] fields = new DBFField[columns.size()];

                for (int i = 0; i <columns.size(); i++) {
                    fields[i] = new DBFField();
                    fields[i].setName(columns.get(i).getString("name"));
                    if (columns.get(i).getString("type").equals("char")){
                        fields[i].setType(DBFDataType.CHARACTER);
                        fields[i].setLength(columns.get(i).getInt("length"));
                    }else if (columns.get(i).getString("type").equals("numeric")){
                        fields[i].setType(DBFDataType.NUMERIC);
                        fields[i].setLength(columns.get(i).getInt("length"));
                        fields[i].setDecimalCount(columns.get(i).getInt("scal"));

                    }else if (columns.get(i).getString("type").equals("date")){
                        fields[i].setType(DBFDataType.DATE);
                    }

                    // Date类型不能设置字段长度，这里没有处理其它没有字段长度的类型

                }
                writer.setFields(fields);
                com.alibaba.datax.common.element.Record record = null;
                while ((record = lineReceiver.getFromReader()) != null) {
                    Object rowData[] = new Object[columns.size()];
                    Column column;
                    for (int i = 0; i <columns.size(); i++) {
                        column = record.getColumn(i);
                        if (null != column.getRawData()) {
                            String colData = column.getRawData().toString();
                            if (columns.get(i).getString("type").equals("numeric")){
                                rowData[i] = Float.valueOf(colData);
                            } else if (columns.get(i).getString("type").equals("char")){
                                //rowData[i] = new String(colData.getBytes("GBK"));
                                rowData[i] = colData;
                            }else if (columns.get(i).getString("type").equals("date")){
                                rowData[i] = new Date(Long.parseLong(colData));
                            }
                        }

                    }
                    writer.addRecord(rowData);
                }
                DBFUtils.close(writer);
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        DbfFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            } catch (IOException ioe) {
                throw DataXException.asDataXException(
                        DbfFileWriterErrorCode.Write_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), ioe);
            }finally {
                DBFUtils.close(writer);
            }
            LOG.info("end do write");
        }

        private String buildFilePath() {
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
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
