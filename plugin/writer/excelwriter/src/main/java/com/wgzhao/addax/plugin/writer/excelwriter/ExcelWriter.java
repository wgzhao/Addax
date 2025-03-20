package com.wgzhao.addax.plugin.writer.excelwriter;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.exception.AddaxException;
import com.wgzhao.addax.core.plugin.RecordReceiver;
import com.wgzhao.addax.core.spi.Writer;
import com.wgzhao.addax.core.util.Configuration;
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

import static com.wgzhao.addax.core.base.Constant.DEFAULT_DATE_FORMAT;
import static com.wgzhao.addax.core.base.Key.FILE_NAME;
import static com.wgzhao.addax.core.base.Key.HEADER;
import static com.wgzhao.addax.core.base.Key.PATH;
import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.EXECUTE_FAIL;
import static com.wgzhao.addax.core.spi.ErrorCode.ILLEGAL_VALUE;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.NOT_SUPPORT_TYPE;
import static com.wgzhao.addax.core.spi.ErrorCode.PERMISSION_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.REQUIRED_VALUE;

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
            dateStyle.setDataFormat(createHelper.createDataFormat().getFormat(DEFAULT_DATE_FORMAT));

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
