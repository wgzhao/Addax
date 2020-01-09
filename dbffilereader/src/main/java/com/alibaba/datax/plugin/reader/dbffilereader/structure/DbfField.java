package com.alibaba.datax.plugin.reader.dbffilereader.structure;

import com.alibaba.datax.plugin.reader.dbffilereader.exception.DbfException;
import com.alibaba.datax.plugin.reader.dbffilereader.utils.DbfUtils;

import java.io.DataInput;
import java.io.IOException;


/**
 * Field descriptor in dbf header (fix 32 bytes for each field)
 * @see <a href="http://www.fship.com/dbfspecs.txt">DBF specification (2a, 2b)</a>
 *
 * @author Sergey Polovko
 */
public class DbfField {

    public static final int HEADER_TERMINATOR = 0x0d;

    private String fieldName;                   /* 0-10  */
    private DbfDataType dataType;               /* 11    */
    private int reserv1;                        /* 12-15 */
    private int fieldLength;                    /* 16    */
    private byte decimalCount;                  /* 17    */
    private short reserv2;                      /* 18-19 */
    private byte workAreaId;                    /* 20    */
    private short reserv3;                      /* 21-22 */
    private byte setFieldsFlag;                 /* 23    */
    private byte[] reserv4 = new byte[7];       /* 24-30 */
    private byte indexFieldFlag;                /* 31    */
    private final int fieldIndex;

    private DbfField(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    /**
     * <p>Creates a DBFField object from the data read from the given DataInputStream.</p>
     * <p>The data in the DataInputStream object is supposed to be organised correctly
     * and the stream "pointer" is supposed to be positioned properly.</p>
     *
     * @param in DataInputStream
     * @return created DBFField object.
     * @throws DbfException if any stream reading problems occurs.
     */
    public static DbfField read(DataInput in, int fieldIndex) throws DbfException {
        try {
            DbfField field = new DbfField(fieldIndex);

            byte firstByte = in.readByte();                     /* 0     */
            if (firstByte == HEADER_TERMINATOR) {
                // we get end of the dbf header
                return null;
            }

            byte[] nameBuf = new byte[11];                      /* 1-10  */
            in.readFully(nameBuf, 1, 10);
            nameBuf[0] = firstByte;

            int zeroIndex = 0;
            while (zeroIndex < nameBuf.length && nameBuf[zeroIndex] != 0) zeroIndex++;
            field.fieldName = new String(nameBuf, 0, zeroIndex);
            byte fieldType  = in.readByte();
            field.dataType = DbfDataType.valueOf(fieldType);    /* 11    */
            if (field.dataType == null) {
                throw new DbfException(
                        String.format(
                                "Unsupported Dbf field type: %s",
                                Integer.toString(fieldType, 16)
                        )
                );
            }
            field.reserv1 = DbfUtils.readLittleEndianInt(in);   /* 12-15 */
            field.fieldLength = in.readUnsignedByte();          /* 16    */
            field.decimalCount = in.readByte();                 /* 17    */
            field.reserv2 = DbfUtils.readLittleEndianShort(in); /* 18-19 */
            field.workAreaId = in.readByte();                   /* 20    */
            field.reserv2 = DbfUtils.readLittleEndianShort(in); /* 21-22 */
            field.setFieldsFlag = in.readByte();                /* 23    */
            in.readFully(field.reserv4);                        /* 24-30 */
            field.indexFieldFlag = in.readByte();               /* 31    */

            return field;
        } catch (IOException e) {
            throw new DbfException("Cannot read Dbf field", e);
        }
    }

    public String getName() {
        return fieldName;
    }

    public DbfDataType getDataType() {
        return dataType;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public int getDecimalCount() {
        return decimalCount;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }
}
