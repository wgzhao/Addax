package com.wgzhao.addax.plugin.writer.excelwriter;

import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.wgzhao.addax.common.base.Key.FILE_NAME;
import static com.wgzhao.addax.common.base.Key.HEADER;
import static com.wgzhao.addax.common.base.Key.PATH;
import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.common.exception.CommonErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.IO_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.common.exception.CommonErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.REQUIRED_VALUE;

public class ExcelWriter
    extends Writer
{
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
            if (fileName.endsWith(".xls")){
                throw AddaxException.asAddaxException(NOT_SUPPORT_TYPE, "Only support new excel format file(.xlsx)");
            }
            if (fileName.split("\\.").length == 1) {
                // no suffix ?
                this.conf.set(FILE_NAME, fileName + ".xlsx");
            }
            try {
                // warn: 这里用户需要配一个目录
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
        public void prepare()
        {

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

        private Configuration conf;

        private String filePath;
        private List<String> header;

        @Override
        public void init()
        {
            this.conf = this.getPluginJobConf();
            this.filePath = this.conf.get(PATH) + "/" + this.conf.get(FILE_NAME);
            this.header =this.conf.getList(HEADER, String.class);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            Record record;
            XSSFWorkbook workbook = new XSSFWorkbook();
            XSSFSheet sheet = workbook.createSheet();
            Row row;
            Cell cell;
            int rowNum = 0;
            // set header ?
            if (!header.isEmpty()) {
                row = sheet.createRow(rowNum++);
                for(int i =0 ;i< header.size();i++) {
                    cell = row.createCell(i);
                    cell.setCellValue(header.get(i));
                }
            }
            // set date format
            CellStyle dateStyle = workbook.createCellStyle();
            CreationHelper createHelper = workbook.getCreationHelper();
            dateStyle.setDataFormat(createHelper.createDataFormat().getFormat("yyyy-MM-dd HH:mm:ss"));

            while ( (record = lineReceiver.getFromReader()) != null)
            {
                int recordLength = record.getColumnNumber();
                row = sheet.createRow(rowNum++);
                Column column;
                for(int i=0; i< recordLength; i++) {
                    cell = row.createCell(i);
                    column = record.getColumn(i);
                    if (column == null || column.getRawData() == null) {
                        cell.setBlank();
                        continue;
                    }
                    switch (column.getType()) {
                        case INT:
                        case LONG:
                            cell.setCellValue(column.asLong());
                            break;
                        case BOOL:
                            cell.setCellValue(column.asBoolean());
                            break;
                        case DATE:
                            cell.setCellValue(column.asDate());
                            cell.setCellStyle(dateStyle);
                            break;
                        case NULL:
                            cell.setBlank();
                            break;
                        default:
                            cell.setCellValue(column.asString());
                    }
                }
            }
            // write to file
            try(FileOutputStream out = new FileOutputStream(filePath)) {
                workbook.write(out);
                workbook.close();
            }
            catch (FileNotFoundException e) {
                throw AddaxException.asAddaxException(CONFIG_ERROR, "No such file: " + filePath);
            }
            catch (IOException e) {
                throw AddaxException.asAddaxException(IO_ERROR, "IOException occurred while writing to " + filePath);
            }
        }
    }
}
