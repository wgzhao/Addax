package com.alibaba.datax.plugin.reader.dbffilereader.structure;

import com.alibaba.datax.plugin.reader.dbffilereader.exception.DbfException;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Date;
import static java.lang.String.format;
import static com.alibaba.datax.plugin.reader.dbffilereader.utils.DbfUtils.trimLeftSpaces;


public class DbfRow {

    private static final Long ZERO = 0L;

    private final DbfHeader header;
    private final Charset defaultCharset;
    private final Object[] row;

    public DbfRow(DbfHeader header, Charset defaultCharset, Object[] row) {
        this.header = header;
        this.defaultCharset = defaultCharset;
        this.row = row;
    }

    /**
     * Retrieves the value of the designated field as java.math.BigDecimal.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public BigDecimal getBigDecimal(String fieldName) throws DbfException {
        Object value = get(fieldName);
        return value == null ? null : new BigDecimal(value.toString());
    }

    /**
     * Retrieves the value of the designated field as java.util.Date.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public Date getDate(String fieldName) throws DbfException {
        return  (Date) get(fieldName);
    }

    /**
     * Retrieves the value of the designated field as String.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public String getString(String fieldName) throws DbfException {
        return getString(fieldName, defaultCharset);
    }

    /**
     * Retrieves the value of the designated field as String
     * using given charset.
     *
     * @param fieldName the name of the field
     * @param charset the charset to be used to decode field value
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public String getString(String fieldName, Charset charset) throws DbfException {
        Object value = get(fieldName);
        return value == null
                ? null
                : new String(trimLeftSpaces((byte[]) value), charset);
    }

    /**
     * Retrieves the value of the designated field as boolean.
     *
     * @param fieldName the name of the field
     * @return the field value, or false (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public boolean getBoolean(String fieldName) throws DbfException {
        Boolean value = (Boolean) get(fieldName);
        return value != null && value;
    }

    /**
     * Retrieves the value of the designated field as int.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public int getInt(String fieldName) throws DbfException {
        return getNumber(fieldName).intValue();
    }

    /**
     * Retrieves the value of the designated field as short.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public short getShort(String fieldName) throws DbfException {
        return getNumber(fieldName).shortValue();
    }

    /**
     * Retrieves the value of the designated field as byte.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public byte getByte(String fieldName) throws DbfException {
        return getNumber(fieldName).byteValue();
    }

    /**
     * Retrieves the value of the designated field as long.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public long getLong(String fieldName) throws DbfException {
        return getNumber(fieldName).longValue();
    }

    /**
     * Retrieves the value of the designated field as float.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public float getFloat(String fieldName) throws DbfException {
        return getNumber(fieldName).floatValue();
    }

    /**
     * Retrieves the value of the designated field as double.
     *
     * @param fieldName the name of the field
     * @return the field value, or 0 (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public double getDouble(String fieldName) throws DbfException {
        return getNumber(fieldName).doubleValue();
    }

    /**
     * Retrieves the value of the designated field as Object.
     *
     * @param fieldName the name of the field
     * @return the field value, or null (if the dbf value is NULL)
     * @throws DbfException if there's no field with name fieldName
     */
    public Object getObject(String fieldName) throws DbfException {
        return get(fieldName);
    }

    private Number getNumber(String fieldName) {
        Number value = (Number) get(fieldName);
        return value == null ? ZERO : value;
    }

    private Object get(String fieldName) {
        int fieldIndex = header.getFieldIndex(fieldName);
        if (fieldIndex < 0) {
            throw new DbfException(format("Field \"%s\" does not exist", fieldName));
        }
        return row[fieldIndex];
    }
}
