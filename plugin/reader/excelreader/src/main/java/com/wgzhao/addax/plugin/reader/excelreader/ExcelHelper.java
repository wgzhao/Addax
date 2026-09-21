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

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.util.LocaleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;

/**
 * What both Excel readers share: which rows to drop, how wide a record is, and how a cell becomes
 * a column.
 *
 * <p>Rows are pushed into a sink while the sheet is parsed rather than pulled one at a time,
 * because the event model reader of the xlsx format gets its rows from a SAX parser and has no way
 * to ask for the next one.
 *
 * <p>A record always has as many columns as the widest row parsed so far, missing cells become
 * nulls. Dropping them instead would shift every following column of that row to the left, which
 * silently corrupts the data instead of failing the job.
 */
abstract class ExcelHelper
        implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ExcelHelper.class);

    /** A cell the sheet has no value for, in the same shape other readers hand out a null. */
    static final Column NULL = new StringColumn();

    final Options options;
    private final int droppedRows;

    private Supplier<Record> recordSupplier;
    private Consumer<Record> sink;
    private Record record;
    private int columnIndex;
    private int width;
    private int rowsParsed;
    private boolean widthWarned;

    ExcelHelper(Options options)
    {
        this.options = options;
        this.droppedRows = (options.header() ? 1 : 0) + Math.max(options.skipRows(), 0);
    }

    /** The settings of the reader plugin that both implementations need. */
    record Options(boolean header, int skipRows, boolean trim, String sheetName, int sheetIndex)
    {
    }

    /**
     * Open the file with the reader that matches its content: xlsx is a zip and is read with the
     * event model, xls is an OLE2 container and is read with the user model, which is affordable
     * there because the format stops at 65536 rows.
     *
     * @param file the file to read
     * @param options the settings the record layout depends on
     * @return the reader for that file
     * @throws IOException when the file cannot be read
     */
    static ExcelHelper open(Path file, Options options)
            throws IOException
    {
        byte[] magic = magic(file);
        if (isZip(magic)) {
            return new XlsxStreamHelper(file, options);
        }
        if (isOle2(magic)) {
            return new XlsDomHelper(file, options);
        }
        throw AddaxException.asAddaxException(CONFIG_ERROR, file + " is neither an xls nor an xlsx file");
    }

    /**
     * Whether a file looks like an Excel workbook. A directory scan picks up whatever lies next to
     * the workbooks, and a text file among them must not fail the job.
     *
     * @param file the file to inspect
     * @return true when the file starts with a workbook magic
     * @throws IOException when the file cannot be read
     */
    static boolean isExcel(Path file)
            throws IOException
    {
        byte[] magic = magic(file);
        return isZip(magic) || isOle2(magic);
    }

    /**
     * Read the whole sheet and hand every record to {@code sink}.
     *
     * @param recordSupplier creates the records to fill, they come from the channel
     * @param sink receives the records that are part of the result
     * @throws IOException when the sheet cannot be read
     */
    final void read(Supplier<Record> recordSupplier, Consumer<Record> sink)
            throws IOException
    {
        this.recordSupplier = recordSupplier;
        this.sink = sink;
        parse();
    }

    /** Read the selected sheet from its first to its last row, row by row. */
    abstract void parse()
            throws IOException;

    /** Start a new record, every following cell belongs to it. */
    void beginRow()
    {
        record = recordSupplier.get();
        columnIndex = 0;
    }

    /**
     * Add the cell to the record at its position in the sheet. Positions the sheet leaves out stay
     * empty, so a cell always ends up in the column its reference names.
     *
     * @param index the zero based column of the cell
     * @param column the value of the cell, {@link #NULL} when it has none
     */
    void putColumn(int index, Column column)
    {
        if (record == null) {
            // a cell outside of a row, only a hand edited sheet does that
            return;
        }
        // a cell behind the current position would be a malformed sheet, appending keeps the data
        while (columnIndex < index) {
            record.addColumn(NULL);
            columnIndex++;
        }
        record.addColumn(column);
        columnIndex++;
    }

    /**
     * Finish the record: drop it when the configuration asks for it, otherwise make it as wide as
     * the widest row and send it on. A row wider than the ones before extends the layout, but the
     * records already sent keep the narrower one, so it is worth a warning.
     */
    void endRow()
    {
        int parsed = rowsParsed++;
        boolean isHeaderRow = parsed < (options.header() ? 1 : 0);
        boolean dropped = parsed < droppedRows;
        // the header row says how many columns the sheet has, the rows dropped by skipRows are
        // usually titles and would only seed a width the data rows do not share
        if (isHeaderRow || !dropped) {
            if (columnIndex > width) {
                if (width > 0 && !widthWarned) {
                    LOG.warn("Row {} has {} columns, more than the {} of the rows before it; "
                                    + "the records already sent keep the narrower layout",
                            parsed + 1, columnIndex, width);
                    widthWarned = true;
                }
                width = columnIndex;
            }
        }
        if (dropped) {
            return;
        }
        while (columnIndex < width) {
            record.addColumn(NULL);
            columnIndex++;
        }
        sink.accept(record);
    }

    /** The text of a text cell, empty cells are nulls like every other cell without a value. */
    static Column stringColumn(String value, boolean trim)
    {
        // strip() and not trim(): trim() only knows the ASCII range, text pasted from a browser
        // carries the wider Unicode spaces. Non breaking spaces are characters of their own and
        // are left alone by either of them.
        String text = trim ? value.strip() : value;
        return text.isEmpty() ? NULL : new StringColumn(text);
    }

    /** A numeric cell: a long for whole numbers, a double for the rest. */
    static Column numberColumn(double value)
    {
        return (long) value == value ? new LongColumn((long) value) : new DoubleColumn(value);
    }

    /**
     * A numeric cell whose format describes a date, which is how Excel stores a date.
     *
     * @param value the day count Excel stored
     * @param date1904 whether the workbook counts the days from 1904 instead of 1900
     */
    static Column dateColumn(double value, boolean date1904)
    {
        return new DateColumn(DateUtil.getJavaDate(value, date1904, LocaleUtil.getUserTimeZone(), false));
    }

    /** Close what was opened for reading, a failure here must not fail the job. */
    protected void closeQuietly(Closeable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (IOException e) {
            LOG.warn("Failed to close {}: {}", closeable, e.getMessage());
        }
    }

    private static byte[] magic(Path file)
            throws IOException
    {
        byte[] magic = new byte[8];
        try (InputStream in = Files.newInputStream(file)) {
            int read = in.readNBytes(magic, 0, magic.length);
            if (read < magic.length) {
                // shorter than any header, keep what was read and let the caller reject it
                byte[] shorter = new byte[Math.max(read, 0)];
                System.arraycopy(magic, 0, shorter, 0, shorter.length);
                return shorter;
            }
        }
        return magic;
    }

    /** Every xlsx starts with the zip magic, the local file header of its first entry. */
    private static boolean isZip(byte[] magic)
    {
        return magic.length >= 4 && magic[0] == 'P' && magic[1] == 'K'
                && magic[2] == 3 && magic[3] == 4;
    }

    /** The old xls format is a compound file, the magic of every OLE2 container. */
    private static boolean isOle2(byte[] magic)
    {
        byte[] ole2 = {(byte) 0xD0, (byte) 0xCF, 0x11, (byte) 0xE0, (byte) 0xA1, (byte) 0xB1, 0x1A, (byte) 0xE1};
        if (magic.length < ole2.length) {
            return false;
        }
        for (int i = 0; i < ole2.length; i++) {
            if (magic[i] != ole2[i]) {
                return false;
            }
        }
        return true;
    }
}
