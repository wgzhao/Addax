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
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.util.LocaleUtil;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

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

/** Excel Writer. */
public class ExcelWriter
        extends Writer
{
    private static final Logger log = LoggerFactory.getLogger(ExcelWriter.class);

    /** Job. */
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
            String path = this.conf.getNecessaryValue(PATH, REQUIRED_VALUE);
            String fileName = this.conf.getNecessaryValue(FILE_NAME, REQUIRED_VALUE);
            if (fileName.endsWith(".xls")) {
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "Only support new excel format file(.xlsx)");
            }
            if (fileName.indexOf('.') < 0) {
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

    /** Task. */
    public static class Task
            extends Writer.Task
    {
        /**
         * Zip levels 1-3 use deflate_fast while 4 and above switch to the much slower deflate_slow.
         * On a 200k x 8 export this took the writer from 0.67 s to 0.41 s and left the file 4%
         * larger, a trade that pays off for an offline batch job writing multi hundred MB sheets.
         */
        private static final int ZIP_COMPRESSION_LEVEL = 3;

        private Path targetFile;
        private List<String> header;

        @Override
        public void init()
        {
            Configuration conf = this.getPluginJobConf();
            this.targetFile = Path.of(conf.getString(PATH), conf.getString(FILE_NAME));
            this.header = conf.getList(HEADER, String.class);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            try (XSSFWorkbook workbook = new XSSFWorkbook()) {
                XSSFSheet sheet = workbook.createSheet();
                //name of the zip entry holding sheet data, e.g. /xl/worksheets/sheet1.xml
                String sheetRef = sheet.getPackagePart().getPartName().getName().substring(1);

                Path template = writeTemplate(workbook);
                // The temp file sits next to the target so the finished file can replace the old one in
                // one step. Its name is built here and not by createTempFile, which would make the file
                // owner-only, while the published file has to end up with the mode a plain file creation
                // gets from the umask - or with the mode the file being replaced already had.
                Path tmp = targetFile.resolveSibling(targetFile.getFileName() + "." + UUID.randomUUID() + ".tmp");
                try {
                    try (OutputStream out = Files.newOutputStream(tmp, StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE)) {
                        writeWorkbook(template, sheetRef, out, workbook, lineReceiver);
                    }
                    keepPermissions(targetFile, tmp);
                    replace(tmp, targetFile);
                }
                finally {
                    deleteQuietly(template);
                    deleteQuietly(tmp);
                }
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR,
                        "IOException occurred while writing to " + targetFile + ": " + e.getMessage(), e);
            }
        }

        /**
         * The template supplies everything the streaming writer cannot generate itself: the styles
         * the date cells refer to, and the workbook and package metadata. Its (empty) sheet entry is
         * the one the row data replaces.
         */
        private Path writeTemplate(XSSFWorkbook workbook)
                throws IOException
        {
            Path template = TempFile.createTempFile("template", ".xlsx").toPath();
            try (OutputStream out = Files.newOutputStream(template)) {
                workbook.write(out);
            }
            return template;
        }

        /**
         * Copy the template and append the generated sheet data as the last entry. The rows go
         * straight into the zip, so the sheet XML never has to be buffered in a file of its own.
         *
         * @param template the template file
         * @param sheetEntry the name of the sheet entry holding the row data, e.g. xl/worksheets/sheet1.xml
         * @param out the stream to write the result to
         */
        private void writeWorkbook(Path template, String sheetEntry, OutputStream out,
                XSSFWorkbook workbook, RecordReceiver lineReceiver)
                throws IOException
        {
            try (ZipFile zip = ZipFile.builder().setPath(template).get();
                    ZipArchiveOutputStream zos = new ZipArchiveOutputStream(out)) {
                zos.setLevel(ZIP_COMPRESSION_LEVEL);
                Enumeration<ZipArchiveEntry> en = zip.getEntries();
                while (en.hasMoreElements()) {
                    ZipArchiveEntry ze = en.nextElement();
                    if (!ze.getName().equals(sheetEntry)) {
                        zos.putArchiveEntry(new ZipArchiveEntry(ze.getName()));
                        try (InputStream is = zip.getInputStream(ze)) {
                            is.transferTo(zos);
                        }
                        zos.closeArchiveEntry();
                    }
                }
                zos.putArchiveEntry(new ZipArchiveEntry(sheetEntry));
                // only flush: closing the writer would close the zip entry stream as well
                java.io.Writer writer = new OutputStreamWriter(zos, StandardCharsets.UTF_8);
                fillData(writer, lineReceiver, workbook);
                writer.flush();
                zos.closeArchiveEntry();
            }
        }

        /**
         * Carry the mode of the file being replaced over to its successor. Writing in place used to
         * keep it, and a job that reruns over an output someone restricted should not widen it.
         */
        private void keepPermissions(Path target, Path tmp)
        {
            try {
                if (Files.exists(target)) {
                    Files.setPosixFilePermissions(tmp, Files.getPosixFilePermissions(target));
                }
            }
            catch (UnsupportedOperationException | IOException e) {
                // no posix permissions to copy on this filesystem
            }
        }

        /**
         * Publish the finished file in one step, so a failed or killed job cannot leave a truncated
         * xlsx behind where the next run expects a complete one.
         */
        private void replace(Path tmp, Path target)
                throws IOException
        {
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            }
            catch (AtomicMoveNotSupportedException e) {
                // network and some windows filesystems cannot move atomically
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        /** Best effort cleanup: a leftover temp file must not outlive the task, but it is not fatal. */
        private void deleteQuietly(Path file)
        {
            try {
                Files.deleteIfExists(file);
            }
            catch (IOException e) {
                log.warn("temp file {} delete failed: {}", file, e.getMessage());
            }
        }

        private void fillData(java.io.Writer writer, RecordReceiver lineReceiver, XSSFWorkbook workbook)
                throws IOException
        {
            SpreadsheetWriter sw = new SpreadsheetWriter(writer);
            sw.beginSheet();
            // row numbers are 0 based and the header occupies the first one
            int rowNum = 0;
            // set header ?
            if (!header.isEmpty()) {
                sw.insertRow(rowNum++);
                for (int i = 0; i < header.size(); i++) {
                    sw.createCell(i, header.get(i));
                }
                sw.endRow();
            }
            // set date format
            CellStyle dateStyle = workbook.createCellStyle();
            CreationHelper createHelper = workbook.getCreationHelper();
            dateStyle.setDataFormat(createHelper.createDataFormat().getFormat(DEFAULT_DATE_FORMAT));
            int dateStyleIndex = dateStyle.getIndex();
            // one calendar for every date cell, a fresh one per cell is pure garbage
            Calendar calendar = LocaleUtil.getLocaleCalendar();
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                sw.insertRow(rowNum++);
                int recordLength = record.getColumnNumber();
                for (int i = 0; i < recordLength; i++) {
                    Column column = record.getColumn(i);
                    if (column == null || column.getRawData() == null) {
                        sw.createCell(i, "");
                        continue;
                    }
                    switch (column.getType()) {
                        case INT, LONG -> sw.createCell(i, column.asLong());
                        case DOUBLE -> writeDouble(sw, i, column);
                        case BOOL -> sw.createCell(i, Boolean.TRUE.equals(column.asBoolean()));
                        case DATE, TIMESTAMP -> {
                            calendar.setTime(column.asDate());
                            sw.createCell(i, calendar, dateStyleIndex);
                        }
                        case NULL -> sw.createCell(i, "");
                        default -> sw.createCell(i, column.asString());
                    }
                }
                sw.endRow();
            }
            sw.endSheet();
        }

        private void writeDouble(SpreadsheetWriter sw, int columnIndex, Column column)
        {
            Double value = column.asDouble();
            if (value == null || !Double.isFinite(value)) {
                // Excel has no way to store NaN or infinity, keep those readable as text
                sw.createCell(columnIndex, column.asString());
            }
            else {
                sw.createCell(columnIndex, value);
            }
        }
    }
}
