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

package com.wgzhao.addax.plugin.reader.excelreader;

import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;

import java.io.PrintStream;

public class RecordHandler
        implements SheetContentsHandler
{
    private boolean firstCellOfRow;
    private int currentRow = -1;
    private int currentCol = -1;

    private final  int minColumns;
    private final PrintStream output;

    public RecordHandler(PrintStream output, int minColumns) {
        this.output = output;
        this.minColumns = minColumns;
    }
    private void outputMissingRows(int number) {
        for (int i=0; i<number; i++) {
            for (int j=0; j<minColumns; j++) {
                output.append(',');
            }
            output.append('\n');
        }
    }

    @Override
    public void startRow(int rowNum) {
        // If there were gaps, output the missing rows
        outputMissingRows(rowNum-currentRow-1);
        // Prepare for this row
        firstCellOfRow = true;
        currentRow = rowNum;
        currentCol = -1;
    }

    @Override
    public void endRow(int rowNum) {
        // Ensure the minimum number of columns
        for (int i=currentCol; i<minColumns; i++) {
            output.append(',');
        }
        output.append('\n');
    }

    @Override
    public void cell(String cellReference, String formattedValue,
            XSSFComment comment) {
        if (firstCellOfRow) {
            firstCellOfRow = false;
        } else {
            output.append(',');
        }

        // gracefully handle missing CellRef here in a similar way as XSSFCell does
        if(cellReference == null) {
            cellReference = new CellAddress(currentRow, currentCol).formatAsString();
        }

        // Did we miss any cells?
        int thisCol = (new CellReference(cellReference)).getCol();
        int missedCols = thisCol - currentCol - 1;
        for (int i=0; i<missedCols; i++) {
            output.append(',');
        }

        // no need to append anything if we do not have a value
        if (formattedValue == null) {
            return;
        }

        currentCol = thisCol;

        // Number or string?
        try {
            //noinspection ResultOfMethodCallIgnored
            Double.parseDouble(formattedValue);
            output.append(formattedValue);
        } catch (Exception e) {
            // let's remove quotes if they are already there
            if (formattedValue.startsWith("\"") && formattedValue.endsWith("\"")) {
                formattedValue = formattedValue.substring(1, formattedValue.length()-1);
            }

            output.append('"');
            // encode double-quote with two double-quotes to produce a valid CSV format
            output.append(formattedValue.replace("\"", "\"\""));
            output.append('"');
        }
    }
}
