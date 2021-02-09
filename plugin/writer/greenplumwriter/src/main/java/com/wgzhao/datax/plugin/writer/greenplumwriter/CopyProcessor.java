package com.wgzhao.datax.plugin.writer.greenplumwriter;

import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.plugin.rdbms.util.DBUtilErrorCode;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CopyProcessor
        implements Callable<Long>
{
    private static final Logger LOG = LoggerFactory.getLogger(CopyProcessor.class);
    private final int columnNumber;
    private final CopyWriterTask task;
    private final LinkedBlockingQueue<Record> queueIn;
    private final LinkedBlockingQueue<byte[]> queueOut;
    private final Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    public CopyProcessor(CopyWriterTask task, int columnNumber,
            Triple<List<String>, List<Integer>, List<String>> resultSetMetaData, LinkedBlockingQueue<Record> queueIn,
            LinkedBlockingQueue<byte[]> queueOut)
    {
        this.task = task;
        this.columnNumber = columnNumber;
        this.resultSetMetaData = resultSetMetaData;
        this.queueIn = queueIn;
        this.queueOut = queueOut;
    }

    @Override
    public Long call()
            throws Exception
    {
        Thread.currentThread().setName("CopyProcessor");
        Record record;

        while (true) {
            record = queueIn.poll(Constant.TIME_OUT_MS, TimeUnit.MILLISECONDS);

            if (record == null && !task.moreRecord()) {
                break;
            }
            else if (record == null) {
                continue;
            }

            if (record.getColumnNumber() != this.columnNumber) {
                // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 您可能配置了错误的表名称, 请检查您的配置并作出修改.",
                                record.getColumnNumber(), this.columnNumber));
            }

            byte[] data = serializeRecord(record);

            if (data.length > Constant.MAX_CSV_SIZE) {
                String s = new String(data).substring(0, 100) + "...";
                LOG.warn("数据元组超过 {} 字节长度限制被忽略。{}", s, Constant.MAX_CSV_SIZE);
            }
            else {
                queueOut.put(data);
            }
        }

        return 0L;
    }

    /**
     * Any occurrence within the value of a QUOTE character or the ESCAPE
     * character is preceded by the escape character.
     */
    protected String escapeString(String data)
    {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length(); ++i) {
            char c = data.charAt(i);
            switch (c) {
                case 0x00:
                    LOG.warn("字符串中发现非法字符 0x00，已经将其删除");
                    continue;
                case Constant.QUOTE_CHAR:
                case Constant.ESCAPE:
                    sb.append(Constant.ESCAPE);
                    break;
                default:
                    break;
            }

            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Non-printable characters are inserted as '\nnn' (octal) and '\' as '\\'.
     */
    protected String escapeBinary(byte[] data)
    {
        StringBuilder sb = new StringBuilder();

        for (byte datum : data) {
            if (datum == Constant.ESCAPE) {
                sb.append(Constant.ESCAPE);
                sb.append(Constant.ESCAPE);
            }
            else if (datum < 0x20 || datum > 0x7e) {
                byte b = datum;
                char[] val = new char[3];
                val[2] = (char) ((b & 7) + '0');
                b >>= 3;
                val[1] = (char) ((b & 7) + '0');
                b >>= 3;
                val[0] = (char) ((b & 3) + '0');
                sb.append('\\');
                sb.append(val);
            }
            else {
                sb.append((char) datum);
            }
        }

        return sb.toString();
    }

    protected byte[] serializeRecord(Record record)
    {
        StringBuilder sb = new StringBuilder();
        Column column;
        for (int i = 0; i < this.columnNumber; i++) {
            column = record.getColumn(i);
            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);

            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    String data = column.asString();

                    if (data != null) {
                        sb.append(Constant.QUOTE_CHAR);
                        sb.append(escapeString(data));
                        sb.append(Constant.QUOTE_CHAR);
                    }

                    break;
                }
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                case Types.LONGVARBINARY:
                case Types.NCLOB:
                case Types.VARBINARY: {
                    byte[] data = column.asBytes();

                    if (data != null) {
                        sb.append(escapeBinary(data));
                    }

                    break;
                }
                default: {
                    String data = column.asString();

                    if (data != null) {
                        sb.append(data);
                    }

                    break;
                }
            }

            if (i + 1 < this.columnNumber) {
                sb.append(Constant.DELIMTER);
            }
        }
        sb.append(Constant.NEWLINE);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
