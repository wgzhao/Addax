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

import com.wgzhao.addax.core.element.BoolColumn;
import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.poi.EmptyFileException;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.UnsupportedFileFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;

/**
 * Reads the xls format with the user model, where POI builds cells the reader can ask for.
 *
 * <p>The older format is the only one this is affordable for: it stops at 65536 rows and 256
 * columns, so the workbook fits in memory even as objects, while the xlsx format has no such bound
 * and is read with the event model instead.
 *
 * <p>It is also the only reader that can evaluate formulas, which is what the xls format needs
 * because Excel did not keep the result of a formula in older versions.
 */
final class XlsDomHelper
        extends ExcelHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(XlsDomHelper.class);

    private final Path file;
    private Workbook workbook;
    private FormulaEvaluator evaluator;
    private boolean formulaWarned;

    XlsDomHelper(Path file, Options options)
    {
        super(options);
        this.file = file;
    }

    @Override
    void parse()
            throws IOException
    {
        workbook = openWorkbook();
        evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        Sheet sheet = selectSheet();
        LOG.info("Read sheet [{}] of {}", sheet.getSheetName(), file);
        for (Row row : sheet) {
            beginRow();
            // cellIterator() only visits the cells the sheet really stores, a cell left out in
            // between would shift every following column of the row to the left
            int columns = row.getLastCellNum();
            for (int i = 0; i < columns; i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                putColumn(i, cell == null ? NULL : toColumn(cell));
            }
            endRow();
        }
    }

    @Override
    public void close()
    {
        closeQuietly(workbook);
    }

    private Workbook openWorkbook()
            throws IOException
    {
        try {
            return WorkbookFactory.create(file.toFile());
        }
        catch (EncryptedDocumentException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    file + " is password protected, it cannot be read", e);
        }
        catch (EmptyFileException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, file + " is empty", e);
        }
        catch (UnsupportedFileFormatException e) {
            // an OLE2 container that holds something else, a doc file for example
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    file + " is not an xls workbook: " + e.getMessage(), e);
        }
    }

    /** Pick the sheet to read and report what was picked, the same way the xlsx reader does. */
    private Sheet selectSheet()
    {
        int sheets = workbook.getNumberOfSheets();
        if (sheets == 0) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, file + " does not contain any sheet");
        }
        if (options.sheetName() != null) {
            Sheet sheet = workbook.getSheet(options.sheetName());
            if (sheet == null) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Sheet '" + options.sheetName() + "' does not exist in " + file + ", it has " + sheetNames());
            }
            return sheet;
        }
        int index = options.sheetIndex();
        if (index < 0 || index >= sheets) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    "sheetIndex " + index + " does not exist in " + file + ", it has " + sheets + " sheet(s)");
        }
        if (index == 0 && sheets > 1) {
            LOG.warn("{} has {} sheets, only the first one '{}' is read; use sheetName or sheetIndex to read another",
                    file, sheets, workbook.getSheetName(0));
        }
        return workbook.getSheetAt(index);
    }

    private List<String> sheetNames()
    {
        List<String> names = new ArrayList<>(workbook.getNumberOfSheets());
        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            names.add(workbook.getSheetName(i));
        }
        return names;
    }

    private Column toColumn(Cell cell)
    {
        CellType type = cell.getCellType();
        if (type == CellType.FORMULA) {
            type = evaluate(cell);
        }
        return switch (type) {
            case NUMERIC -> DateUtil.isCellDateFormatted(cell)
                    ? new DateColumn(cell.getDateCellValue())
                    : numberColumn(cell.getNumericCellValue());
            case STRING -> stringColumn(cell.getStringCellValue(), options.trim());
            case BOOLEAN -> new BoolColumn(cell.getBooleanCellValue());
            // an error cell holds no data, the same as an empty one
            case ERROR, BLANK, _NONE, FORMULA -> NULL;
        };
    }

    /**
     * Evaluate a formula cell, falling back to the result stored in the file. POI does not know
     * every function Excel writes (TEXTJOIN and the other {@code _xlfn} functions for example), and
     * a single one of them must not fail the job; Excel stored what it computed, so that value is
     * still there to be read.
     */
    private CellType evaluate(Cell cell)
    {
        try {
            return evaluator.evaluateFormulaCell(cell);
        }
        catch (RuntimeException e) {
            if (!formulaWarned) {
                LOG.warn("{} contains a formula that cannot be evaluated ({}), the result stored in the file is read instead",
                        file, e.getMessage());
                formulaWarned = true;
            }
            return cell.getCachedFormulaResultType();
        }
    }
}
