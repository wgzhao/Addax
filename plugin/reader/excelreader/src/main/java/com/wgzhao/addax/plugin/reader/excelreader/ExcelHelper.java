/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.wgzhao.addax.plugin.reader.excelreader;

import com.wgzhao.addax.common.element.BoolColumn;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import static com.wgzhao.addax.common.spi.ErrorCode.IO_ERROR;

public class ExcelHelper
{
    public boolean header;
    public int skipRows;
    FileInputStream file;
    Workbook workbook;
    private FormulaEvaluator evaluator;
    private Iterator<Row> rowIterator;

    public ExcelHelper(boolean header, int skipRows) {
        this.header = header;
        this.skipRows = skipRows;
    }
    public void open(String filePath)
    {
        try {
            this.file = new FileInputStream(filePath);
            workbook = WorkbookFactory.create(file);
            // ONLY read the first sheet
            Sheet sheet = workbook.getSheetAt(0);
            this.evaluator =  workbook.getCreationHelper().createFormulaEvaluator();
            this.rowIterator = sheet.iterator();
            if (this.header && this.rowIterator.hasNext()) {
                // skip header
                this.rowIterator.next();
            }
            if (this.skipRows > 0) {
                int i =0;
                while (this.rowIterator.hasNext() && i < this.skipRows) {
                    this.rowIterator.next();
                    i++;
                }
            }
        }
        catch (FileNotFoundException e) {
            throw AddaxException.asAddaxException(IO_ERROR, e);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "IOException occurred when open '" + filePath + "':" + e.getMessage());
        }
    }

    public void close()
    {
        try {
            this.workbook.close();
            this.file.close();
        }
        catch (IOException ignored) {

        }
    }

    public Record readLine(Record record)
    {
        if (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            //For each row, iterate through all the columns
            Iterator<Cell> cellIterator = row.cellIterator();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                //Check the cell type after evaluating formulae
                //If it is formula cell, it will be evaluated otherwise no change will happen
                switch (evaluator.evaluateInCell(cell).getCellType()) {
                    case NUMERIC:
                        // numeric include whole numbers, fractional numbers, dates
                        if (DateUtil.isCellDateFormatted(cell)) {
                            record.addColumn(new DateColumn(cell.getDateCellValue()));
                        } else {
                            // integer or long ?
                            double a = cell.getNumericCellValue();
                            if ((long) a == a) {
                                record.addColumn(new LongColumn((long) a));
                            } else {
                                record.addColumn(new DoubleColumn(a));
                            }
                        }
                        break;
                    case STRING:
                        record.addColumn(new StringColumn(cell.getStringCellValue().trim()));
                        break;
                    case BOOLEAN:
                        record.addColumn(new BoolColumn(cell.getBooleanCellValue()));
                        break;
                    case FORMULA:
                    case _NONE:
                        break;
                    case ERROR:
                        // #VALUE!
                        record.addColumn(new StringColumn());
                        break;
                    case BLANK:
                        // empty cell
                        record.addColumn(new StringColumn(""));
                        break;
                }
            }
            return record;
        }
        else {
            return null;
        }
    }
}
