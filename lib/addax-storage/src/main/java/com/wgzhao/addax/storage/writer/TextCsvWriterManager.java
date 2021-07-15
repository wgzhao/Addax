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

package com.wgzhao.addax.storage.writer;

import com.csvreader.CsvWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_FILE_FORMAT;

public class TextCsvWriterManager
{
    public static Writer produceUnstructuredWriter(
            String fileFormat, char fieldDelimiter, java.io.Writer writer)
    {
        // warn: false means plain text(old way), true means strict csv format
        if (DEFAULT_FILE_FORMAT.equals(fileFormat)) {
            return new TextWriterImpl(writer, fieldDelimiter);
        }
        else {
            return new CsvWriterImpl(writer, fieldDelimiter);
        }
    }
}

class CsvWriterImpl
        implements Writer
{
    private static final Logger LOG = LoggerFactory
            .getLogger(CsvWriterImpl.class);
    private final CsvWriter csvWriter;

    public CsvWriterImpl(java.io.Writer writer, char fieldDelimiter)
    {
        // csv 严格符合csv语法, 有标准的转义等处理
        this.csvWriter = new CsvWriter(writer, fieldDelimiter);
        this.csvWriter.setTextQualifier('"');
        this.csvWriter.setUseTextQualifier(true);
        // warn: in linux is \n , in windows is \r\n
        this.csvWriter.setRecordDelimiter(IOUtils.LINE_SEPARATOR_UNIX.charAt(0));
    }

    @Override
    public void writeOneRecord(List<String> splitedRows)
            throws IOException
    {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        this.csvWriter.writeRecord(splitedRows
                .toArray(new String[0]));
    }

    @Override
    public void flush()
    {
        this.csvWriter.flush();
    }

    @Override
    public void close()
    {
        this.csvWriter.close();
    }
}

class TextWriterImpl
        implements Writer
{
    private static final Logger LOG = LoggerFactory
            .getLogger(TextWriterImpl.class);
    // text StringUtils的join方式, 简单的字符串拼接
    private final char fieldDelimiter;
    private final java.io.Writer textWriter;

    public TextWriterImpl(java.io.Writer writer, char fieldDelimiter)
    {
        this.fieldDelimiter = fieldDelimiter;
        this.textWriter = writer;
    }

    @Override
    public void writeOneRecord(List<String> splitedRows)
            throws IOException
    {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        this.textWriter.write(String.format("%s%s",
                StringUtils.join(splitedRows, this.fieldDelimiter),
                IOUtils.LINE_SEPARATOR_UNIX));
    }

    @Override
    public void flush()
            throws IOException
    {
        this.textWriter.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        this.textWriter.close();
    }
}
