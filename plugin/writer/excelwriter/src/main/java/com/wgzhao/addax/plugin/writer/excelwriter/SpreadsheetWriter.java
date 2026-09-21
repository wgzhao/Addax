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
import java.util.Arrays;
import java.util.Calendar;

/**
 * Writes spreadsheet data in a Writer.
 * (YK: in future it may evolve in a full-featured API for streaming data in Excel)
 */
public class SpreadsheetWriter
{
    /** Above 2^53 a long no longer survives the double that Excel stores every number as. */
    private static final double MAX_EXACT_INTEGER = 1L << 53;

    private final Writer out;

    /** One row is assembled here and written in a single call, the writer would otherwise encode
     *  every start tag, attribute and value separately. */
    private final StringBuilder row = new StringBuilder(512);

    /** Column letters only depend on the column index, so they are computed once and reused. */
    private String[] columnLetters = new String[32];

    private int rownum;

    SpreadsheetWriter(Writer out)
    {
        this.out = out;
    }

    void beginSheet()
            throws IOException
    {
        out.write("""
                <?xml version="1.0" encoding="UTF-8"?>
                <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
                <sheetData>
                """);
    }

    void endSheet()
            throws IOException
    {
        out.write("""
                </sheetData>
                </worksheet>
                """);
    }

    /**
     * Insert a new row
     *
     * @param rownum 0-based row number
     */
    void insertRow(int rownum)
    {
        row.setLength(0);
        row.append("<row r=\"").append(rownum + 1).append("\">\n");
        this.rownum = rownum;
    }

    /**
     * Insert row end marker
     */
    void endRow()
            throws IOException
    {
        row.append("</row>\n");
        out.write(row.toString());
    }

    /** Createcell. */
    public void createCell(int columnIndex, String value, int styleIndex)
    {
        row.append("<c r=\"").append(columnLetter(columnIndex)).append(rownum + 1).append("\" t=\"inlineStr\"");
        if (styleIndex != -1) {
            row.append(" s=\"").append(styleIndex).append('"');
        }
        row.append("><is><t>").append(escapeXml(value)).append("</t></is></c>");
    }

    /** Createcell. */
    public void createCell(int columnIndex, String value)
    {
        createCell(columnIndex, value, -1);
    }

    /** Createcell. */
    public void createCell(int columnIndex, double value, int styleIndex)
    {
        row.append("<c r=\"").append(columnLetter(columnIndex)).append(rownum + 1).append("\" t=\"n\"");
        if (styleIndex != -1) {
            row.append(" s=\"").append(styleIndex).append('"');
        }
        row.append("><v>");
        appendNumber(row, value);
        row.append("</v></c>");
    }

    /** Createcell. */
    public void createCell(int columnIndex, double value)
    {
        createCell(columnIndex, value, -1);
    }

    /** Createcell. */
    public void createCell(int columnIndex, boolean value)
    {
        row.append("<c r=\"").append(columnLetter(columnIndex)).append(rownum + 1)
                .append("\" t=\"b\"><v>").append(value ? 1 : 0).append("</v></c>");
    }

    /** Createcell. */
    public void createCell(int columnIndex, Calendar value, int styleIndex)
    {
        createCell(columnIndex, DateUtil.getExcelDate(value, false), styleIndex);
    }

    private String columnLetter(int columnIndex)
    {
        if (columnIndex >= columnLetters.length) {
            columnLetters = Arrays.copyOf(columnLetters, Math.max(columnIndex + 1, columnLetters.length * 2));
        }
        String letters = columnLetters[columnIndex];
        if (letters == null) {
            letters = CellReference.convertNumToColString(columnIndex);
            columnLetters[columnIndex] = letters;
        }
        return letters;
    }

    /**
     * Excel reads every number as a double, so printing an integral value as {@code 101} rather than
     * {@code 101.0} changes nothing in the sheet; the cast is preferred over
     * {@link Double#toString} because that switches to scientific notation from 1e7 on.
     */
    private static void appendNumber(StringBuilder target, double value)
    {
        if (value == Math.rint(value) && Math.abs(value) <= MAX_EXACT_INTEGER) {
            target.append((long) value);
        }
        else {
            target.append(value);
        }
    }

    /**
     * Escape a value for XML element content.
     * <p>
     * Besides the markup characters this handles two cases a hand written sheet gets wrong: XML 1.0
     * forbids most control characters (and unpaired surrogates) inside a document, and Excel decodes
     * a literal {@code _xHHHH_} back into the character it stands for, so an underscore that starts
     * such a code has to be written as {@code _x005F_}.
     */
    private static String escapeXml(String value)
    {
        if (value == null || value.isEmpty()) {
            return "";
        }
        int length = value.length();
        StringBuilder escaped = null;
        int copied = 0;
        for (int i = 0; i < length; ) {
            int codePoint = value.codePointAt(i);
            int width = Character.charCount(codePoint);
            String replacement;
            switch (codePoint) {
                case '&' -> replacement = "&amp;";
                case '<' -> replacement = "&lt;";
                case '>' -> replacement = "&gt;";
                case '"' -> replacement = "&quot;";
                case '\'' -> replacement = "&apos;";
                case '_' -> replacement = isExcelEscape(value, i) ? "_x005F_" : null;
                default -> replacement = isAllowedXmlChar(codePoint) ? null : "?";
            }
            if (replacement != null) {
                if (escaped == null) {
                    escaped = new StringBuilder(length + 16);
                }
                escaped.append(value, copied, i).append(replacement);
                copied = i + width;
            }
            i += width;
        }
        if (escaped == null) {
            return value;
        }
        return escaped.append(value, copied, length).toString();
    }

    /**
     * The characters XML 1.0 allows in element content, see <a href="https://www.w3.org/TR/xml/#charsets">the spec</a>.
     * A lone surrogate is not a valid code point and is rejected here as well.
     */
    private static boolean isAllowedXmlChar(int codePoint)
    {
        return codePoint == 0x9 || codePoint == 0xA || codePoint == 0xD
                || (codePoint >= 0x20 && codePoint <= 0xD7FF)
                || (codePoint >= 0xE000 && codePoint <= 0xFFFD)
                || (codePoint >= 0x10000 && codePoint <= 0x10FFFF);
    }

    /** True when the underscore at {@code index} starts a literal _xHHHH_ code. */
    private static boolean isExcelEscape(String value, int index)
    {
        if (index + 6 >= value.length() || value.charAt(index + 1) != 'x') {
            return false;
        }
        for (int i = index + 2; i < index + 6; i++) {
            char ch = value.charAt(i);
            if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F'))) {
                return false;
            }
        }
        return value.charAt(index + 6) == '_';
    }
}
