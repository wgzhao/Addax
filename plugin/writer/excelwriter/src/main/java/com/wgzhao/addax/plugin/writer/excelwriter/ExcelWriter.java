/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.wgzhao.addax.plugin.writer.excelwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.poi.openxml4j.opc.internal.ZipHelper;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.util.LocaleUtil;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static com.wgzhao.addax.core.base.Constant.DEFAULT_DATE_FORMAT;
import static com.wgzhao.addax.core.base.Key.FILE_NAME;
import static com.wgzhao.addax.core.base.Key.HEADER;
import static com.wgzhao.addax.core.base.Key.PATH;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

public class ExcelWriter
        extends Writer
{
    private static final Logger log = LoggerFactory.getLogger(ExcelWriter.class);

    public static class Job
            extends Writer.Job
    {
        private Configuration conf;

        @Override
        public void init()
        {
            this.conf = this.getPluginJobConf();
            this.validateParameter();
        }

        private void validateParameter()
        {
            this.conf.getNecessaryValue(PATH, REQUIRED_VALUE);
            String path = this.conf.getNecessaryValue(PATH, REQUIRED_VALUE);
            String fileName = this.conf.getNecessaryValue(FILE_NAME, REQUIRED_VALUE);
            if (fileName.endsWith(".xls")) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "Only support new excel format file(.xlsx)");
            }
            if (fileName.split("\\.").length == 1) {
                // no suffix ?
                this.conf.set(FILE_NAME, fileName + ".xlsx");
            }
            try {
                File dir = new File(path);
                if (dir.isFile()) {
                    throw AddaxException.asAddaxException(ILLEGAL_VALUE, path + " is normal file instead of directory");
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw AddaxException.asAddaxException(EXECUTE_FAIL,
                                "can not create directory '" + dir + "' failure");
                    }
                }
            }
            catch (SecurityException se) {
                throw AddaxException.asAddaxException(PERMISSION_ERROR,
                        "Create directory '" + path + "' failure: permission deny: ", se);
            }
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            // only ONE thread
            return Collections.singletonList(this.conf);
        }
    }

    public static class Task
            extends Writer.Task
    {

        private String filePath;
        private List<String> header;
        private XSSFWorkbook workbook;

        @Override
        public void init()
        {
            Configuration conf = this.getPluginJobConf();
            this.filePath = conf.get(PATH) + "/" + conf.get(FILE_NAME);
            this.header = conf.getList(HEADER, String.class);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            this.workbook = new XSSFWorkbook();
            XSSFSheet sheet = workbook.createSheet();
            //name of the zip entry holding sheet data, e.g. /xl/worksheets/sheet1.xml
            String sheetRef = sheet.getPackagePart().getPartName().getName();

            // save the template
            File fTemplate;
            try {
                fTemplate = TempFile.createTempFile("template", ".xlsx");
                FileOutputStream fileOutputStream = new FileOutputStream(fTemplate);
                workbook.write(fileOutputStream);
                fileOutputStream.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            File tmp = null;
            try {
                //Step 2. Generate XML file.
                tmp = TempFile.createTempFile("sheet", ".xml");
                log.info("temp file for sheet data: {}", tmp.getAbsolutePath());
                FileOutputStream stream = new FileOutputStream(tmp);
                java.io.Writer fw = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
                fillData(fw, lineReceiver);
                fw.close();
                stream.close();

                //Step 3. Substitute the template entry with the generated data
                FileOutputStream out = new FileOutputStream(this.filePath);
                substitute(fTemplate, tmp, sheetRef.substring(1), out);
                out.close();

                workbook.close();
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR, "IOException occurred while writing to " + filePath);
            }
            finally {
                // delete the temp file
                if (tmp != null && tmp.exists()) {
                    if (!tmp.delete()) {
                        log.warn("temp file {} delete failed.", tmp.getAbsolutePath());
                    }
                }
                if (fTemplate.exists()) {
                    if (!fTemplate.delete()) {
                        log.warn("temp file {} delete failed.", fTemplate.getAbsolutePath());
                    }
                }
            }
        }

        private void fillData(java.io.Writer writer, RecordReceiver lineReceiver)
                throws IOException
        {
            int rowNum = 0;
            SpreadsheetWriter sw = new SpreadsheetWriter(writer);
            sw.beginSheet();
            // set header ?
            if (!header.isEmpty()) {
                sw.insertRow(0);
                for (int i = 0; i < header.size(); i++) {
                    sw.createCell(i, header.get(i));
                }
                sw.endRow();
            }
            // set date format
            CellStyle dateStyle = workbook.createCellStyle();
            CreationHelper createHelper = workbook.getCreationHelper();
            dateStyle.setDataFormat(createHelper.createDataFormat().getFormat(DEFAULT_DATE_FORMAT));
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                int recordLength = record.getColumnNumber();
                sw.insertRow(rowNum++);
                Column column;
                for (int i = 0; i < recordLength; i++) {
                    column = record.getColumn(i);
                    if (column == null || column.getRawData() == null) {
                        sw.createCell(i, "");
                        continue;
                    }
                    switch (column.getType()) {
                        case INT:
                        case LONG:
                            sw.createCell(i, column.asLong());
                            break;
                        case BOOL:
                            sw.createCell(i, column.asBoolean().toString());
                            break;
                        case DATE:
                            Calendar calendar = LocaleUtil.getLocaleCalendar();
                            calendar.setTime(column.asDate());
                            sw.createCell(i, calendar, dateStyle.getIndex());
                            break;
                        case NULL:
                            sw.createCell(i, "");
                            break;
                        default:
                            sw.createCell(i, column.asString());
                    }
                }
                sw.endRow();
            }
            sw.endSheet();
        }

        /**
         *
         * @param zipfile the template file
         * @param tmpfile the XML file with the sheet data
         * @param entry the name of the sheet entry to substitute, e.g. xl/worksheets/sheet1.xml
         * @param out the stream to write the result to
         */
        private void substitute(File zipfile, File tmpfile, String entry, OutputStream out)
                throws IOException
        {
            try (ZipFile zip = ZipHelper.openZipFile(zipfile)) {
                try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(out)) {
                    Enumeration<? extends ZipArchiveEntry> en = zip.getEntries();
                    while (en.hasMoreElements()) {
                        ZipArchiveEntry ze = en.nextElement();
                        if (!ze.getName().equals(entry)) {
                            zos.putArchiveEntry(new ZipArchiveEntry(ze.getName()));
                            try (InputStream is = zip.getInputStream(ze)) {
                                copyStream(is, zos);
                            }
                            zos.closeArchiveEntry();
                        }
                    }
                    zos.putArchiveEntry(new ZipArchiveEntry(entry));
                    try (InputStream is = new FileInputStream(tmpfile)) {
                        copyStream(is, zos);
                    }
                    zos.closeArchiveEntry();
                }
            }
        }

        private void copyStream(InputStream in, OutputStream out)
                throws IOException
        {
            byte[] chunk = new byte[1024];
            int count;
            while ((count = in.read(chunk)) >= 0) {
                out.write(chunk, 0, count);
            }
        }
    }
}
