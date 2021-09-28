package com.wgzhao.addax.common.element;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.exception.CommonErrorCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

public class TimestampColumn
    extends Column
{
    public TimestampColumn(Object object, Type type, int byteSize)
    {
        super(object, type, byteSize);
    }

    public TimestampColumn() {
        this((Timestamp)null);
    }

    public TimestampColumn(Timestamp ts)
    {
        super(ts, Type.TIMESTAMP, (ts == null ? 0 : 12));
    }

    public TimestampColumn(Long ts)
    {
        this(new Timestamp(ts));
    }

    public TimestampColumn(String ts)
    {
        this(Timestamp.valueOf(ts));
    }

    public TimestampColumn(Date ts)
    {
        this(Timestamp.from(ts.toInstant()));
    }

    @Override
    public Long asLong()
    {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asTimestamp().getTime();
    }

    public Timestamp asTimestamp()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return (Timestamp)  this.getRawData();
    }

    @Override
    public Double asDouble()
    {
       if (null == this.getRawData()) {
           return null;
       }
       return (Double) this.getRawData();
    }

    @Override
    public String asString()
    {
        if (null == this.getRawData()) {
            return null;
        }

        Timestamp ts = this.asTimestamp();
        return ts.toString();
    }

    @Override
    public Date asDate()
    {
        if (null == this.getRawData()) {
            return null;
        }
        return new Date(this.asLong());
    }

    @Override
    public byte[] asBytes()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Timestamp类型不能转为Bytes .");
    }

    @Override
    public Boolean asBoolean()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Timestamp类型不能转为Boolean .");
    }

    @Override
    public BigDecimal asBigDecimal()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Timestamp类型不能转为BigDecimal .");
    }

    @Override
    public BigInteger asBigInteger()
    {
        throw AddaxException.asAddaxException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Timestamp类型不能转为BigInteger .");
    }

    public static void main(String[] args)
            throws SQLException
    {
        String s = "2021-09-27 11:12:13.123456";
        Connection connection = DriverManager.getConnection("jdbc:mysql://10.60.172.153:3306/test", "wbuser", "wbuser123");
        Statement statement = connection.createStatement();
        statement.executeQuery("truncate table addax_write");
        PreparedStatement preparedStatement = connection.prepareStatement("insert into addax_write values(?)");
        Column column = new TimestampColumn(s);
        preparedStatement.setTimestamp(1, column.asTimestamp());
        preparedStatement.execute();
        preparedStatement.close();
        connection.close();
    }
}
