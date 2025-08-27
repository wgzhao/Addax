/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.excelwriter;

import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.util.CellReference;

import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;

/**
 * Writes spreadsheet data in a Writer.
 * (YK: in future it may evolve in a full-featured API for streaming data in Excel)
 */
public class SpreadsheetWriter
{
    private final Writer _out;
    private int _rownum;

    SpreadsheetWriter(Writer out)
    {
        _out = out;
    }

    void beginSheet()
            throws IOException
    {
        _out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">");
        _out.write("<sheetData>\n");
    }

    void endSheet()
            throws IOException
    {
        _out.write("</sheetData>");
        _out.write("</worksheet>");
    }

    /**
     * Insert a new row
     *
     * @param rownum 0-based row number
     */
    void insertRow(int rownum)
            throws IOException
    {
        _out.write("<row r=\"" + (rownum + 1) + "\">\n");
        this._rownum = rownum;
    }

    /**
     * Insert row end marker
     */
    void endRow()
            throws IOException
    {
        _out.write("</row>\n");
    }

    public void createCell(int columnIndex, String value, int styleIndex)
            throws IOException
    {
        String ref = new CellReference(_rownum, columnIndex).formatAsString();
        _out.write("<c r=\"" + ref + "\" t=\"inlineStr\"");
        if (styleIndex != -1) {
            _out.write(" s=\"" + styleIndex + "\"");
        }
        _out.write(">");
        // value must be XML-escaped
        _out.write("<is><t>" + escapeXml(value) + "</t></is>");
        _out.write("</c>");
    }

    public void createCell(int columnIndex, String value)
            throws IOException
    {
        createCell(columnIndex, value, -1);
    }

    public void createCell(int columnIndex, double value, int styleIndex)
            throws IOException
    {
        String ref = new CellReference(_rownum, columnIndex).formatAsString();
        _out.write("<c r=\"" + ref + "\" t=\"n\"");
        if (styleIndex != -1) {
            _out.write(" s=\"" + styleIndex + "\"");
        }
        _out.write(">");
        _out.write("<v>" + value + "</v>");
        _out.write("</c>");
    }

    public void createCell(int columnIndex, double value)
            throws IOException
    {
        createCell(columnIndex, value, -1);
    }

    public void createCell(int columnIndex, Calendar value, int styleIndex)
            throws IOException
    {
        createCell(columnIndex, DateUtil.getExcelDate(value, false), styleIndex);
    }

    private String escapeXml(String value)
    {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }
}