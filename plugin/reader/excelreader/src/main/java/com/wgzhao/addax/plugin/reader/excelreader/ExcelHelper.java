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
import com.wgzhao.addax.core.element.DateColumn;
import com.wgzhao.addax.core.element.DoubleColumn;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.exception.AddaxException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.wgzhao.addax.core.spi.ErrorCode.IO_ERROR;

public class ExcelHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(ExcelHelper.class);

    public boolean hasHeader;
    public int skipRows;
    FileInputStream file;
    Workbook workbook;
    private FormulaEvaluator evaluator;
    private Iterator<Row> rowIterator;

    // Add memory limit configuration, default 500MB
    private static final int DEFAULT_MAX_MEMORY_BYTES = 500 * 1024 * 1024;
    private final int maxMemoryBytes;

    // Streaming read support
    private boolean useStreamingRead = false;
    private BlockingQueue<List<String>> rowQueue;
    private volatile boolean streamingComplete = false;
    private volatile Exception streamingException = null;
    private Thread streamingThread;
    private static final String STREAMING_END_MARKER = "__STREAMING_END__";

    public ExcelHelper(boolean header, int skipRows) {
        this(header, skipRows, DEFAULT_MAX_MEMORY_BYTES);
    }

    public ExcelHelper(boolean header, int skipRows, int maxMemoryBytes) {
        this.hasHeader = header;
        this.skipRows = skipRows;
        this.maxMemoryBytes = maxMemoryBytes;
        // Set POI byte array max override value to handle large files
        IOUtils.setByteArrayMaxOverride(this.maxMemoryBytes);
    }

    public void open(String filePath)
    {
        try {
            this.file = new FileInputStream(filePath);

            // First try to determine if we should use streaming read based on file size
            long fileSize = this.file.getChannel().size();
            boolean shouldUseStreaming = fileSize > 20 * 1024 * 1024; // Files larger than 20MB

            LOG.info("Opening Excel file: {} (size: {}MB)", filePath, fileSize / (1024 * 1024));
            LOG.info("Should use streaming: {} (file size > 20MB: {})", shouldUseStreaming, fileSize > 20 * 1024 * 1024);

            if (shouldUseStreaming && filePath.toLowerCase().endsWith(".xlsx")) {
                LOG.info("Large XLSX file detected ({}MB), using streaming read mode", fileSize / (1024 * 1024));
                initStreamingRead(filePath);
                return;
            }

            // Try to use standard read mode for smaller files or XLS files
            try {
                LOG.info("Attempting standard workbook creation");
                workbook = WorkbookFactory.create(file);
                initStandardRead();
                LOG.info("Standard read mode initialized successfully");
            } catch (Exception e) {
                // If standard read fails and it's an XLSX file, try streaming
                if (filePath.toLowerCase().endsWith(".xlsx")) {
                    LOG.warn("Standard workbook creation failed, switching to streaming mode. Error: {}", e.getMessage());
                    file.close();
                    initStreamingRead(filePath);
                } else {
                    // For XLS files, try with increased memory limit
                    LOG.warn("Standard workbook creation failed, retrying with increased memory limit. Error: {}", e.getMessage());
                    file.close();
                    this.file = new FileInputStream(filePath);
                    workbook = createWorkbookForLargeFile(file);
                    initStandardRead();
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
        catch (Exception e) {
            throw AddaxException.asAddaxException(IO_ERROR,
                    "Failed to open Excel file '" + filePath + "'. " +
                    "If this is a very large file, consider splitting it into smaller files or increasing maxMemoryMB setting. Error: " + e.getMessage());
        }
    }

    private void initStandardRead() {
        LOG.info("Initializing standard read mode");
        // ONLY read the first sheet
        Sheet sheet = workbook.getSheetAt(0);
        this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        this.rowIterator = sheet.iterator();

        int totalRows = sheet.getLastRowNum() + 1;
        LOG.info("Sheet has {} rows total", totalRows);
        LOG.info("hasHeader: {}, skipRows: {}", hasHeader, skipRows);

        if (this.hasHeader && this.rowIterator.hasNext()) {
            // skip header
            this.rowIterator.next();
            LOG.info("Skipped header row");
        }
        if (this.skipRows > 0) {
            int i = 0;
            while (this.rowIterator.hasNext() && i < this.skipRows) {
                this.rowIterator.next();
                i++;
            }
            LOG.info("Skipped {} additional rows", this.skipRows);
        }
        LOG.info("Standard read mode initialization completed");
    }

    private void initStreamingRead(String filePath) throws IOException {
        LOG.info("Initializing streaming read mode for file: {}", filePath);
        this.useStreamingRead = true;
        this.rowQueue = new ArrayBlockingQueue<>(1000); // Buffer 1000 rows

        // Start streaming read in background thread
        this.streamingThread = new Thread(() -> {
            LOG.info("Streaming read thread started");
            try {
                performStreamingRead(filePath);
                LOG.info("Streaming read completed successfully");
            } catch (Exception e) {
                this.streamingException = e;
                LOG.error("Error in streaming read thread", e);
            } finally {
                this.streamingComplete = true;
                // Add end marker to signal completion
                try {
                    List<String> endMarker = new ArrayList<>();
                    endMarker.add(STREAMING_END_MARKER);
                    this.rowQueue.put(endMarker);
                    LOG.info("End marker added to queue");
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        this.streamingThread.start();
        LOG.info("Streaming thread started successfully");
    }

    private void performStreamingRead(String filePath) throws Exception {
        LOG.info("Starting performStreamingRead for file: {}", filePath);
        try (FileInputStream fis = new FileInputStream(filePath);
             OPCPackage opcPackage = OPCPackage.open(fis)) {

            LOG.info("OPCPackage opened successfully");
            XSSFReader reader = new XSSFReader(opcPackage);
            org.apache.poi.xssf.model.SharedStrings sst = reader.getSharedStringsTable();
            StylesTable styles = reader.getStylesTable();
            LOG.info("SharedStrings and StylesTable loaded");

            // Get the first sheet
            Iterator<InputStream> sheets = reader.getSheetsData();
            LOG.info("Got sheets iterator, hasNext: {}", sheets.hasNext());
            if (sheets.hasNext()) {
                InputStream sheetInputStream = sheets.next();
                LOG.info("Got first sheet input stream");

                StreamingSheetHandler handler = new StreamingSheetHandler();
                LOG.info("Created StreamingSheetHandler with hasHeader: {}, skipRows: {}", hasHeader, skipRows);

                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                LOG.info("SAX parser created");

                // Try different XSSFSheetXMLHandler constructor patterns based on POI version
                ContentHandler contentHandler;
                try {
                    // Try newer POI version constructor (styles, sst, handler, dataOnly)
                    contentHandler = new XSSFSheetXMLHandler(styles, sst, handler, false);
                    LOG.info("Using XSSFSheetXMLHandler constructor: (styles, sst, handler, dataOnly)");
                } catch (Exception e) {
                    try {
                        // Try older POI version constructor (styles, null, sst, handler, dataOnly)
                        contentHandler = new XSSFSheetXMLHandler(styles, null, sst, handler, false);
                        LOG.info("Using XSSFSheetXMLHandler constructor: (styles, null, sst, handler, dataOnly)");
                    } catch (Exception e2) {
                        try {
                            // Try another variant with different parameter order
                            contentHandler = new XSSFSheetXMLHandler(styles, sst, handler, true);
                            LOG.info("Using XSSFSheetXMLHandler constructor: (styles, sst, handler, true)");
                        } catch (Exception e3) {
                            // Last resort: try with cast to SharedStringsTable
                            contentHandler = new XSSFSheetXMLHandler(styles, (SharedStringsTable)sst, handler, false);
                            LOG.info("Using XSSFSheetXMLHandler constructor: (styles, (SharedStringsTable)sst, handler, false)");
                        }
                    }
                }

                xmlReader.setContentHandler(contentHandler);
                LOG.info("ContentHandler set, starting XML parsing");

                xmlReader.parse(new InputSource(sheetInputStream));
                LOG.info("XML parsing completed");

                sheetInputStream.close();
                LOG.info("Sheet input stream closed");
            } else {
                LOG.warn("No sheets found in the Excel file");
            }
        }
        LOG.info("performStreamingRead completed");
    }

    private class StreamingSheetHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
        private List<String> currentRow;
        private int skipCount = 0;
        private int totalRowsProcessed = 0;

        public StreamingSheetHandler() {
            LOG.info("StreamingSheetHandler created");
        }

        @Override
        public void startRow(int rowNum) {
            LOG.debug("startRow called for row {}", rowNum);
            this.currentRow = new ArrayList<>();
            totalRowsProcessed++;
        }

        @Override
        public void endRow(int rowNum) {
            LOG.debug("endRow called for row {}, currentRow size: {}, currentRow content: {}",
                     rowNum, currentRow.size(), currentRow);

            // Skip header if needed
            if (hasHeader && rowNum == 0) {
                LOG.info("Skipping header row {}", rowNum);
                return;
            }

            // Skip additional rows if needed
            if (skipCount < skipRows) {
                skipCount++;
                LOG.info("Skipping row {} (skipCount: {}/{})", rowNum, skipCount, skipRows);
                return;
            }

            LOG.info("Adding row {} to queue with {} cells: {}", rowNum, currentRow.size(), currentRow);
            try {
                // Add row to queue (blocking if queue is full)
                rowQueue.put(new ArrayList<>(currentRow));
                LOG.debug("Row {} successfully added to queue", rowNum);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while adding row to queue", e);
            }
        }

        @Override
        public void cell(String cellReference, String formattedValue, org.apache.poi.xssf.usermodel.XSSFComment comment) {
            LOG.debug("cell called: cellReference={}, formattedValue='{}', comment={}",
                     cellReference, formattedValue, comment);

            // Ensure the list is large enough for this cell
            int colIndex = getColumnIndex(cellReference);
            while (currentRow.size() <= colIndex) {
                currentRow.add("");
            }

            if (formattedValue != null) {
                currentRow.set(colIndex, formattedValue);
                LOG.debug("Set cell at index {} to value '{}'", colIndex, formattedValue);
            } else {
                LOG.debug("Cell {} has null value", cellReference);
            }
        }

        @Override
        public void headerFooter(String text, boolean isHeader, String tagName) {
            LOG.debug("headerFooter called: text='{}', isHeader={}, tagName='{}'", text, isHeader, tagName);
            // Not needed for data reading
        }

        private int getColumnIndex(String cellReference) {
            // Extract column letters from cell reference (e.g., "A1" -> "A")
            String columnLetters = cellReference.replaceAll("[0-9]", "");
            int column = 0;
            for (int i = 0; i < columnLetters.length(); i++) {
                column = column * 26 + (columnLetters.charAt(i) - 'A' + 1);
            }
            return column - 1; // Convert to 0-based index
        }
    }

    /**
     * Create workbook for large files, prioritize streaming read
     */
    private Workbook createWorkbookForLargeFile(FileInputStream file) throws IOException {
        try {
            // First try standard approach
            return WorkbookFactory.create(file);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("maximum length")) {
                // If it's a memory limit issue, increase limit and retry
                IOUtils.setByteArrayMaxOverride(this.maxMemoryBytes * 2); // Increase to double
                try {
                    return WorkbookFactory.create(file);
                } catch (Exception ex) {
                    // If it fails again, try larger limit
                    IOUtils.setByteArrayMaxOverride(this.maxMemoryBytes * 4); // Increase to quadruple
                    try {
                        return WorkbookFactory.create(file);
                    } catch (Exception ex2) {
                        // Last attempt, remove limit (but this may cause OOM)
                        IOUtils.setByteArrayMaxOverride(-1); // Remove limit
                        return WorkbookFactory.create(file);
                    }
                }
            }
            throw e;
        }
    }

    public void close()
    {
        try {
            if (this.useStreamingRead) {
                if (this.streamingThread != null && this.streamingThread.isAlive()) {
                    this.streamingThread.interrupt();
                    try {
                        this.streamingThread.join(5000); // Wait up to 5 seconds
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } else {
                if (this.workbook != null) {
                    this.workbook.close();
                }
            }

            if (this.file != null) {
                this.file.close();
            }
        }
        catch (IOException ignored) {
            // Ignore close errors
        }
    }

    public Record readLine(Record record)
    {
        LOG.debug("readLine called, useStreamingRead: {}", useStreamingRead);
        if (this.useStreamingRead) {
            return readLineFromStream(record);
        } else {
            return readLineFromIterator(record);
        }
    }

    private Record readLineFromStream(Record record) {
        LOG.debug("readLineFromStream called");
        try {
            // Check for exceptions in streaming thread
            if (streamingException != null) {
                LOG.error("Streaming exception detected: {}", streamingException.getMessage());
                throw new RuntimeException("Error in streaming read", streamingException);
            }

            LOG.debug("Taking row from queue, queue size: {}", rowQueue.size());
            List<String> rowData = rowQueue.take(); // Blocking call
            LOG.debug("Retrieved row data: {}", rowData);

            // Check for end marker
            if (rowData.size() == 1 && STREAMING_END_MARKER.equals(rowData.get(0))) {
                LOG.info("End of stream marker detected, no more data");
                return null; // End of data
            }

            // Convert string data to appropriate column types with better type detection
            for (String cellValue : rowData) {
                if (cellValue == null || cellValue.trim().isEmpty()) {
                    record.addColumn(new StringColumn(""));
                } else {
                    // Try to parse as different types with improved detection
                    record.addColumn(parseStringToColumn(cellValue.trim()));
                }
            }

            LOG.debug("Converted row to record with {} columns", record.getColumnNumber());
            return record;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while reading from stream");
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private com.wgzhao.addax.core.element.Column parseStringToColumn(String value) {
        // Try to detect and convert data types even in streaming mode
        if (value == null || value.isEmpty()) {
            return new StringColumn("");
        }

        // Try to parse as number (long or double)
        try {
            if (value.contains(".")) {
                double doubleVal = Double.parseDouble(value);
                return new DoubleColumn(doubleVal);
            } else {
                long longVal = Long.parseLong(value);
                return new LongColumn(longVal);
            }
        } catch (NumberFormatException e) {
            // Not a number
        }

        // Try to parse as boolean
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return new BoolColumn(Boolean.parseBoolean(value));
        }

        // Try to parse as date (common date formats)
        try {
            // This is a simplified date parsing - you might want to add more formats
            if (value.matches("\\d{4}-\\d{2}-\\d{2}.*") || value.matches("\\d{2}/\\d{2}/\\d{4}.*")) {
                java.text.SimpleDateFormat[] dateFormats = {
                    new java.text.SimpleDateFormat("yyyy-MM-dd"),
                    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
                    new java.text.SimpleDateFormat("MM/dd/yyyy"),
                    new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
                };

                for (java.text.SimpleDateFormat format : dateFormats) {
                    try {
                        java.util.Date date = format.parse(value);
                        return new DateColumn(date);
                    } catch (java.text.ParseException pe) {
                        // Try next format
                    }
                }
            }
        } catch (Exception e) {
            // Not a date
        }

        // Default to string
        return new StringColumn(value);
    }

    private Record readLineFromIterator(Record record) {
        LOG.debug("readLineFromIterator called, rowIterator.hasNext(): {}", rowIterator.hasNext());
        if (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            LOG.debug("Reading row {}, with {} cells", row.getRowNum(), row.getLastCellNum());

            //For each row, iterate through all the columns
            Iterator<Cell> cellIterator = row.cellIterator();
            int cellCount = 0;
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                cellCount++;
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
            LOG.debug("Processed {} cells, record now has {} columns", cellCount, record.getColumnNumber());
            return record;
        }
        else {
            LOG.debug("No more rows available in iterator");
            return null;
        }
    }
}
