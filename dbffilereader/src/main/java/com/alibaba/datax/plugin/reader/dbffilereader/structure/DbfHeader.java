package com.alibaba.datax.plugin.reader.dbffilereader.structure;


import com.alibaba.datax.plugin.reader.dbffilereader.exception.DbfException;
import com.alibaba.datax.plugin.reader.dbffilereader.utils.DbfUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DBF Header (variable size, depending on field count)
 * @see <a href="http://www.fship.com/dbfspecs.txt">DBF specification (2)</a>
 *
 * @author Sergey Polovko
 */
public class DbfHeader {

    private byte signature;              /* 0     */
    private byte year;                   /* 1     */
    private byte month;                  /* 2     */
    private byte day;                    /* 3     */
    private int numberOfRecords;         /* 4-7   */
    private short headerLength;          /* 8-9   */
    private short recordLength;          /* 10-11 */
    private short reserv1;               /* 12-13 */
    private byte incompleteTransaction;  /* 14    */
    private byte encryptionFlag;         /* 15    */
    private int freeRecordThread;        /* 16-19 */
    private int reserv2;                 /* 20-23 */
    private int reserv3;                 /* 24-27 */
    private byte mdxFlag;                /* 28    */
    private byte languageDriver;         /* 29    */
    private short reserv4;               /* 30-31 */
    private List<DbfField> fields;       /* each 32 bytes */

    private Map<String, Integer> fieldIndexesByNames;


    public static DbfHeader read(DataInput dataInput) throws DbfException {
        try {
            DbfHeader header = new DbfHeader();

            header.signature = dataInput.readByte();                           /* 0     */
            header.year = dataInput.readByte();                                /* 1     */
            header.month = dataInput.readByte();                               /* 2     */
            header.day = dataInput.readByte();                                 /* 3     */
            header.numberOfRecords = DbfUtils.readLittleEndianInt(dataInput);  /* 4-7   */

            header.headerLength = DbfUtils.readLittleEndianShort(dataInput);   /* 8-9   */
            header.recordLength = DbfUtils.readLittleEndianShort(dataInput);   /* 10-11 */

            header.reserv1 = DbfUtils.readLittleEndianShort(dataInput);        /* 12-13 */
            header.incompleteTransaction = dataInput.readByte();               /* 14    */
            header.encryptionFlag = dataInput.readByte();                      /* 15    */
            header.freeRecordThread = DbfUtils.readLittleEndianInt(dataInput); /* 16-19 */
            header.reserv2 = dataInput.readInt();                              /* 20-23 */
            header.reserv3 = dataInput.readInt();                              /* 24-27 */
            header.mdxFlag = dataInput.readByte();                             /* 28    */
            header.languageDriver = dataInput.readByte();                      /* 29    */
            header.reserv4 = DbfUtils.readLittleEndianShort(dataInput);        /* 30-31 */

            header.fields = new ArrayList<>();
            DbfField field;
            int fieldIndex = 0;
            while ((field = DbfField.read(dataInput, fieldIndex++)) != null) { /* 32 each */
                header.fields.add(field);
            }

            return header;
        } catch (IOException e) {
            throw new DbfException("Cannot read Dbf header", e);
        }
    }

    public short getHeaderLength() {
        return headerLength;
    }

    public int getFieldsCount() {
        return fields.size();
    }

    public byte getYear() {
        return year;
    }

    public byte getMonth() {
        return month;
    }

    public byte getDay() {
        return day;
    }

    public DbfField getField(int i) {
        return fields.get(i);
    }

    public DbfField getField(String fieldName) {
        return getField(getFieldIndex(fieldName));
    }

    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    public short getRecordLength() {
        return recordLength;
    }

    public int getFieldIndex(String fieldName) {
        if (fieldIndexesByNames == null) {
            initFieldIndexesByNames();
        }
        Integer index = fieldIndexesByNames.get(fieldName);
        return index == null ? -1 : index;
    }

    private void initFieldIndexesByNames() {
        fieldIndexesByNames = new HashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            DbfField field = fields.get(i);
            fieldIndexesByNames.put(field.getName(), i);
        }
    }
}
