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
import org.apache.poi.UnsupportedFileFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.util.LocaleUtil;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;

/**
 * Reads the xlsx format with the event model: the sheet XML is pushed through a SAX parser and only
 * one row is held at a time, so the memory needed does not grow with the number of rows.
 *
 * <p>The user model is not an option here: it keeps the whole sheet as a tree of XMLBeans objects
 * (several gigabytes for half a million rows) and only lends a formula evaluator in return. A
 * workbook saved by Excel carries the result of every formula next to it, and that stored result is
 * what the event model reads.
 */
final class XlsxStreamHelper
        extends ExcelHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(XlsxStreamHelper.class);

    /** Namespace of the relationship attributes, like the {@code r:id} of a sheet. */
    private static final String RELATIONSHIP_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

    /** The xlsx format stops at this many columns, a wider {@code col} element is a broken file. */
    private static final int MAX_COLUMNS = 16384;

    private final Path file;
    private OPCPackage pkg;
    private boolean formulaWarned;

    XlsxStreamHelper(Path file, Options options)
    {
        super(options);
        this.file = file;
    }

    @Override
    void parse()
            throws IOException
    {
        try {
            pkg = OPCPackage.open(file.toFile(), PackageAccess.READ);
            XSSFReader reader = new XSSFReader(pkg);
            // the read only table keeps the strings as plain text instead of building rich text
            // runs for every one of them
            reader.setUseReadOnlySharedStringsTable(true);
            SharedStrings strings = reader.getSharedStringsTable();
            StylesTable styles = reader.getStylesTable();
            WorkbookMeta meta = readWorkbookMeta(reader.getWorkbookData());
            try (InputStream sheet = openSheet(reader, meta)) {
                SAXParser parser = newParserFactory().newSAXParser();
                parser.parse(new InputSource(sheet), new SheetHandler(strings, styles, meta.date1904()));
            }
        }
        catch (UnsupportedFileFormatException e) {
            // a zip that is not a workbook, an ods file for example
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    file + " is not an xlsx file: " + e.getMessage(), e);
        }
        catch (ParserConfigurationException | SAXException | OpenXML4JException e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to read " + file + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close()
    {
        closeQuietly(pkg);
    }

    /**
     * Pick the sheet to read and report what was picked. Reading the first sheet of a workbook that
     * holds more than one is easy to miss in the job result, so it is said out loud.
     */
    private InputStream openSheet(XSSFReader reader, WorkbookMeta meta)
            throws IOException, OpenXML4JException
    {
        List<String> names = meta.sheetNames();
        if (names.isEmpty()) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, file + " does not contain any sheet");
        }
        int index;
        if (options.sheetName() != null) {
            index = names.indexOf(options.sheetName());
            if (index < 0) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "Sheet '" + options.sheetName() + "' does not exist in " + file + ", it has " + names);
            }
        }
        else {
            index = options.sheetIndex();
            if (index < 0 || index >= names.size()) {
                throw AddaxException.asAddaxException(CONFIG_ERROR,
                        "sheetIndex " + index + " does not exist in " + file + ", it has " + names.size() + " sheet(s)");
            }
            if (index == 0 && names.size() > 1) {
                LOG.warn("{} has {} sheets, only the first one '{}' is read; use sheetName or sheetIndex to read another",
                        file, names.size(), names.get(0));
            }
        }
        String id = meta.sheetIds().get(index);
        if (id == null) {
            // without the relationship of the sheet there is no part to read
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    file + " does not name the part of sheet '" + names.get(index) + "'");
        }
        LOG.info("Read sheet [{}] of {}", names.get(index), file);
        try {
            return reader.getSheet(id);
        }
        catch (IllegalArgumentException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR,
                    file + " does not contain the part of sheet '" + names.get(index) + "'", e);
        }
    }

    /** The workbook part tells which year the date serials count from and which sheets exist. */
    private static WorkbookMeta readWorkbookMeta(InputStream workbookData)
            throws IOException, ParserConfigurationException, SAXException
    {
        List<String> names = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        boolean[] date1904 = {false};
        newParserFactory().newSAXParser().parse(new InputSource(workbookData), new DefaultHandler()
        {
            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes)
            {
                switch (localName) {
                    case "workbookPr" -> date1904[0] = isTrue(attributes.getValue("date1904"));
                    case "sheet" -> {
                        String id = attributes.getValue(RELATIONSHIP_NS, "id");
                        if (id == null) {
                            id = attributes.getValue("r:id");
                        }
                        names.add(attributes.getValue("name"));
                        ids.add(id);
                    }
                    default -> {
                    }
                }
            }
        });
        return new WorkbookMeta(date1904[0], names, ids);
    }

    /**
     * A parser that refuses anything a sheet cannot contain: a sheet is plain data, so a document
     * declaring entities has no business being parsed.
     */
    private static SAXParserFactory newParserFactory()
            throws ParserConfigurationException, SAXException
    {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        return factory;
    }

    private static boolean isTrue(String value)
    {
        return "1".equals(value) || "true".equalsIgnoreCase(value);
    }

    /** What the workbook part says about the sheets and the day the date serials start at. */
    private record WorkbookMeta(boolean date1904, List<String> sheetNames, List<String> sheetIds)
    {
    }

    /**
     * Turns the sheet XML into records. The document is a flat sequence of rows holding cells, all
     * the parser has to remember is the cell it is inside of.
     */
    private final class SheetHandler
            extends DefaultHandler
    {
        private final SharedStrings strings;
        private final StylesTable styles;
        private final boolean date1904;

        /** Column formats set by a {@code col} element, they apply to cells without a style of
         *  their own and are the only way a date format can hang off a whole column. */
        private final Map<Integer, Integer> columnStyles = new HashMap<>();
        private final StringBuilder text = new StringBuilder();

        private String reference;
        private String type;
        private int styleIndex = -1;
        private boolean formula;
        private boolean inValue;
        private boolean inInlineString;
        private boolean inText;
        private boolean inPhonetic;
        private int nextColumn;

        SheetHandler(SharedStrings strings, StylesTable styles, boolean date1904)
        {
            this.strings = strings;
            this.styles = styles;
            this.date1904 = date1904;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
        {
            switch (localName) {
                case "row" -> beginRow();
                case "c" -> {
                    reference = attributes.getValue("r");
                    type = attributes.getValue("t");
                    styleIndex = intValue(attributes.getValue("s"), -1);
                    text.setLength(0);
                    formula = false;
                }
                case "v" -> {
                    inValue = true;
                    text.setLength(0);
                }
                case "is" -> inInlineString = true;
                // the runs of a rich text string are separate t elements, so the text is collected
                // until the enclosing is element ends
                case "t" -> inText = inInlineString;
                case "rPh" -> inPhonetic = true;
                case "f" -> formula = true;
                case "col" -> addColumnStyle(attributes);
                default -> {
                }
            }
        }

        @Override
        public void characters(char[] ch, int start, int length)
        {
            if ((inValue || inText) && !inPhonetic) {
                text.append(ch, start, length);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName)
        {
            switch (localName) {
                case "v" -> inValue = false;
                case "t" -> inText = false;
                case "is" -> inInlineString = false;
                case "rPh" -> inPhonetic = false;
                case "c" -> endCell();
                case "row" -> endRow();
                default -> {
                }
            }
        }

        /** The style of a column, the elements are one based and inclusive on both ends. */
        private void addColumnStyle(Attributes attributes)
        {
            int style = intValue(attributes.getValue("style"), -1);
            if (style < 0) {
                return;
            }
            int min = intValue(attributes.getValue("min"), 1);
            int max = Math.min(intValue(attributes.getValue("max"), min), MAX_COLUMNS);
            for (int i = Math.max(min, 1); i <= max; i++) {
                columnStyles.put(i - 1, style);
            }
        }

        private void endCell()
        {
            // the reference is what places the cell; a file without one gets the cells in the order
            // they appear
            int index = reference == null ? nextColumn : columnIndex(reference);
            if (index < 0) {
                index = nextColumn;
            }
            nextColumn = index + 1;
            // a cell without a style of its own inherits the one of its column
            int style = styleIndex >= 0 ? styleIndex : columnStyles.getOrDefault(index, -1);
            putColumn(index, toColumn(style));
            reference = null;
            type = null;
            styleIndex = -1;
            formula = false;
        }

        private Column toColumn(int style)
        {
            String value = text.toString();
            // a number is either marked as one or carries no type at all
            if (type == null || "n".equals(type)) {
                return number(value, style);
            }
            return switch (type) {
                case "s" -> sharedString(value);
                case "inlineStr", "str" -> stringColumn(value, options.trim());
                case "b" -> new BoolColumn("1".equals(value) || "true".equalsIgnoreCase(value));
                // a cell that failed to compute holds no data, the same as an empty one
                case "e" -> NULL;
                case "d" -> date(value);
                default -> NULL;
            };
        }

        private Column number(String value, int style)
        {
            if (value.isEmpty()) {
                // a formula whose result Excel did not store, or a formatted but empty cell
                if (formula && !formulaWarned) {
                    LOG.warn("{} holds a formula without a stored result, the cell is read as null; "
                            + "open and save the file in Excel to fill it in", file);
                    formulaWarned = true;
                }
                return NULL;
            }
            try {
                double number = Double.parseDouble(value);
                boolean date = isDateStyle(style) && DateUtil.isValidExcelDate(number);
                return date ? dateColumn(number, date1904) : numberColumn(number);
            }
            catch (NumberFormatException e) {
                return NULL;
            }
        }

        private Column sharedString(String index)
        {
            int idx = intValue(index, -1);
            if (idx < 0 || idx >= strings.getCount()) {
                return NULL;
            }
            return stringColumn(strings.getItemAt(idx).getString(), options.trim());
        }

        /** xlsx allows an ISO 8601 string in a cell instead of the day count Excel writes. */
        private Column date(String value)
        {
            LocalDateTime dateTime = parseDateTime(value);
            if (dateTime == null) {
                return stringColumn(value, options.trim());
            }
            // the same time zone the day counts are turned into instants with
            return new DateColumn(Date.from(dateTime.atZone(LocaleUtil.getUserTimeZone().toZoneId()).toInstant()));
        }

        private boolean isDateStyle(int style)
        {
            if (style < 0 || style >= styles.getNumCellStyles()) {
                return false;
            }
            XSSFCellStyle cellStyle = styles.getStyleAt(style);
            return cellStyle != null
                    && DateUtil.isADateFormat(cellStyle.getDataFormat(), cellStyle.getDataFormatString());
        }
    }

    /** An ISO 8601 value, which may carry an offset or no time at all. */
    private static LocalDateTime parseDateTime(String value)
    {
        try {
            return LocalDateTime.parse(value);
        }
        catch (DateTimeParseException ignored) {
            // fall through to the other shapes of an ISO date
        }
        try {
            return OffsetDateTime.parse(value).toLocalDateTime();
        }
        catch (DateTimeParseException ignored) {
            // fall through to a date without a time
        }
        try {
            return LocalDate.parse(value).atStartOfDay();
        }
        catch (DateTimeParseException ignored) {
            return null;
        }
    }

    /**
     * The zero based column of a reference like {@code C12}.
     *
     * @param reference the reference of a cell
     * @return the column index, -1 when the reference holds no column letters
     */
    private static int columnIndex(String reference)
    {
        int index = 0;
        for (int i = 0; i < reference.length(); i++) {
            char ch = Character.toUpperCase(reference.charAt(i));
            if (ch == '$') {
                continue;
            }
            if (ch < 'A' || ch > 'Z') {
                break;
            }
            index = index * 26 + (ch - 'A' + 1);
        }
        return index - 1;
    }

    private static int intValue(String value, int defaultValue)
    {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
