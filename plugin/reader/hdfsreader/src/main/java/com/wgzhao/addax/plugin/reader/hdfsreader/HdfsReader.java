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

package com.wgzhao.addax.plugin.reader.hdfsreader;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.storage.reader.StorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.common.base.Key.ENCODING;
import static com.wgzhao.addax.common.base.Key.INDEX;
import static com.wgzhao.addax.common.base.Key.TYPE;
import static com.wgzhao.addax.common.base.Key.VALUE;

public class HdfsReader
        extends Reader
{

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init--&gt;prepare--&gt;split
     * Task类init--&gt;prepare--&gt;startRead--&gt;post--&gt;destroy
     * Task类init--&gt;prepare--&gt;startRead--&gt;post--&gt;destroy
     * Job类post--&gt;destroy
     * </pre>
     */
    public static class Job
            extends Reader.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerOriginConfig = null;
        private HashSet<String> sourceFiles;
        private String specifiedFileType = null;
        private DFSUtil dfsUtil = null;
        private List<String> path = null;

        @Override
        public void init()
        {

            LOG.info("init() begin...");
            this.readerOriginConfig = getPluginJobConf();
            this.validate();
            dfsUtil = new DFSUtil(this.readerOriginConfig);
            LOG.info("init() ok and end...");
        }

        public void validate()
        {
            this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HdfsReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);

            // path check
            String pathInString = this.readerOriginConfig.getNecessaryValue(Key.PATH, HdfsReaderErrorCode.REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<>();
                path.add(pathInString);
            }
            else {
                path = this.readerOriginConfig.getList(Key.PATH, String.class);
                if (null == path || path.isEmpty()) {
                    throw AddaxException.asAddaxException(HdfsReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                }
                for (String eachPath : path) {
                    if (!eachPath.startsWith("/")) {
                        String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                        LOG.error(message);
                        throw AddaxException.asAddaxException(HdfsReaderErrorCode.ILLEGAL_VALUE, message);
                    }
                }
            }

            specifiedFileType = this.readerOriginConfig.getNecessaryValue(Key.FILE_TYPE, HdfsReaderErrorCode.REQUIRED_VALUE).toUpperCase();
            if (!HdfsConstant.SUPPORT_FILE_TYPE.contains(specifiedFileType)) {
                String message = "HdfsReader插件目前支持 " + HdfsConstant.SUPPORT_FILE_TYPE + "几种格式的文件,请将fileType选项的值配置为以上各种的一种";
                throw AddaxException.asAddaxException(HdfsReaderErrorCode.FILE_TYPE_ERROR, message);
            }

            String encoding = this.readerOriginConfig.getString(ENCODING, "UTF-8");

            try {
                Charsets.toCharset(encoding);
            }
            catch (UnsupportedCharsetException uce) {
                throw AddaxException.asAddaxException(
                        HdfsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("不支持的编码格式 : [%s]", encoding), uce);
            }
            catch (Exception e) {
                throw AddaxException.asAddaxException(
                        HdfsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("运行配置异常 : %s", e.getMessage()), e);
            }
            //check Kerberos
            boolean haveKerberos = this.readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsReaderErrorCode.REQUIRED_VALUE);
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsReaderErrorCode.REQUIRED_VALUE);
            }

            // validate the Columns
            validateColumns();

            if (this.specifiedFileType.equals(HdfsConstant.TEXT)
                    || this.specifiedFileType.equals(HdfsConstant.CSV)) {
                //compress校验
                StorageReaderUtil.validateCompress(this.readerOriginConfig);
                StorageReaderUtil.validateCsvReaderConfig(this.readerOriginConfig);
            }
        }

        private void validateColumns()
        {

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig
                    .getListConfiguration(COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                    .equals(column.get(0).toString()))) {
                readerOriginConfig
                        .set(COLUMN, new ArrayList<String>());
            }
            else {
                // column: 1. index type 2.value type 3.when type is Data, may have format
                List<Configuration> columns = this.readerOriginConfig
                        .getListConfiguration(COLUMN);

                if (null == columns || columns.isEmpty()) {
                    throw AddaxException.asAddaxException(
                            HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                for (Configuration eachColumnConf : columns) {
                    eachColumnConf.getNecessaryValue(TYPE, HdfsReaderErrorCode.REQUIRED_VALUE);
                    Integer columnIndex = eachColumnConf.getInt(INDEX);
                    String columnValue = eachColumnConf.getString(VALUE);

                    if (null == columnIndex && null == columnValue) {
                        throw AddaxException.asAddaxException(
                                HdfsReaderErrorCode.NO_INDEX_VALUE,
                                "由于您配置了type, 则至少需要配置 index 或 value");
                    }

                    if (null != columnIndex && null != columnValue) {
                        throw AddaxException.asAddaxException(
                                HdfsReaderErrorCode.MIXED_INDEX_VALUE,
                                "您混合配置了index, value, 每一列同时仅能选择其中一种");
                    }
                }
            }
        }

        @Override
        public void prepare()
        {
            LOG.info("prepare(), start to getAllFiles...");
            this.sourceFiles = (HashSet<String>) dfsUtil.getAllFiles(path, specifiedFileType);
            LOG.info("您即将读取的文件数为: [{}], 列表为: [{}]", this.sourceFiles.size(), this.sourceFiles);
        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {

            LOG.info("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();
            // warn:每个slice拖且仅拖一个文件,
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw AddaxException.asAddaxException(HdfsReaderErrorCode.EMPTY_DIR_EXCEPTION,
                        String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.readerOriginConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(new ArrayList<>(this.sourceFiles), splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(HdfsConstant.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }

            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitSourceFiles(List<T> sourceList, int adviceNumber)
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

    public static class Task
            extends Reader.Task
    {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private List<String> sourceFiles;
        private String specifiedFileType;
        private DFSUtil dfsUtil = null;

        @Override
        public void init()
        {

            this.taskConfig = getPluginJobConf();
            this.sourceFiles = this.taskConfig.getList(HdfsConstant.SOURCE_FILES, String.class);
            this.specifiedFileType = this.taskConfig.getNecessaryValue(Key.FILE_TYPE, HdfsReaderErrorCode.REQUIRED_VALUE);
            this.dfsUtil = new DFSUtil(this.taskConfig);
        }

        @Override
        public void prepare()
        {
            //
        }

        @Override
        public void startRead(RecordSender recordSender)
        {

            LOG.info("read start");
            for (String sourceFile : this.sourceFiles) {
                LOG.info("reading file : [{}]", sourceFile);

                if (specifiedFileType.equalsIgnoreCase(HdfsConstant.TEXT)
                        || specifiedFileType.equalsIgnoreCase(HdfsConstant.CSV)) {

                    InputStream inputStream = dfsUtil.getInputStream(sourceFile);
                    StorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig,
                            recordSender, this.getTaskPluginCollector());
                }
                else if (specifiedFileType.equalsIgnoreCase(HdfsConstant.ORC)) {

                    dfsUtil.orcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                }
                else if (specifiedFileType.equalsIgnoreCase(HdfsConstant.SEQ)) {

                    dfsUtil.sequenceFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                }
                else if (specifiedFileType.equalsIgnoreCase(HdfsConstant.RC)) {

                    dfsUtil.rcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                }
                else if (specifiedFileType.equalsIgnoreCase(HdfsConstant.PARQUET)) {
                    dfsUtil.parquetFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                }
                else {

                    String message = "HdfsReader插件目前支持ORC, TEXT, CSV, SEQUENCE, RC五种格式的文件," +
                            "请将fileType选项的值配置为ORC, TEXT, CSV, SEQUENCE 或者 RC";
                    throw AddaxException.asAddaxException(HdfsReaderErrorCode.FILE_TYPE_UNSUPPORTED, message);
                }

                if (recordSender != null) {
                    recordSender.flush();
                }
            }

            LOG.info("end read source files...");
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