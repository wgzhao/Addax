package com.wgzhao.datax.core.transport.transformer;

import com.wgzhao.datax.common.element.BoolColumn;
import com.wgzhao.datax.common.element.BytesColumn;
import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.DateColumn;
import com.wgzhao.datax.common.element.DoubleColumn;
import com.wgzhao.datax.common.element.LongColumn;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.transformer.Transformer;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class FilterTransformer
        extends Transformer
{
    public FilterTransformer()
    {
        setTransformerName("dx_filter");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {

        int columnIndex;
        String code;
        String value;

        try {
            if (paras.length != 3) {
                throw new RuntimeException("dx_filter paras must be 3");
            }

            columnIndex = (Integer) paras[0];
            code = (String) paras[1];
            value = (String) paras[2];

            if (StringUtils.isEmpty(value)) {
                throw new RuntimeException("dx_filter para 2 can't be null");
            }
        }
        catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                    "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {

            if ("like".equalsIgnoreCase(code)) {
                return doLike(record, value, column);
            }
            else if ("not like".equalsIgnoreCase(code)) {
                return doNotLike(record, value, column);
            }
            else if (">".equalsIgnoreCase(code)) {
                return doGreat(record, value, column, false);
            }
            else if ("<".equalsIgnoreCase(code)) {
                return doLess(record, value, column, false);
            }
            else if ("=".equalsIgnoreCase(code) || "==".equalsIgnoreCase(code)) {
                return doEqual(record, value, column);
            }
            else if ("!=".equalsIgnoreCase(code)) {
                return doNotEqual(record, value, column);
            }
            else if (">=".equalsIgnoreCase(code)) {
                return doGreat(record, value, column, true);
            }
            else if ("<=".equalsIgnoreCase(code)) {
                return doLess(record, value, column, true);
            }
            else {
                throw new RuntimeException("dx_filter can't suport code:" + code);
            }
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
    }

    private Record doGreat(Record record, String value, Column column, boolean hasEqual)
    {

        //如果字段为空，直接不参与比较。即空也属于无穷小
        if (column.getRawData() == null) {
            return record;
        }
        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (hasEqual) {
                if (ori >= val) {
                    return null;
                }
                else {
                    return record;
                }
            }
            else {
                if (ori > val) {
                    return null;
                }
                else {
                    return record;
                }
            }
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (hasEqual) {
                if (ori >= val) {
                    return null;
                }
                else {
                    return record;
                }
            }
            else {
                if (ori > val) {
                    return null;
                }
                else {
                    return record;
                }
            }
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            String ori = column.asString();
            if (hasEqual) {
                if (ori.compareTo(value) >= 0) {
                    return null;
                }
                else {
                    return record;
                }
            }
            else {
                if (ori.compareTo(value) > 0) {
                    return null;
                }
                else {
                    return record;
                }
            }
        }
        else {
            throw new RuntimeException(">=,> can't support this columnType:"
                    + column.getClass().getSimpleName());
        }
    }

    private Record doLess(Record record, String value, Column column, boolean hasEqual)
    {

        //如果字段为空，直接不参与比较。即空也属于无穷大
        if (column.getRawData() == null) {
            return record;
        }

        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (hasEqual) {
                if (ori <= val) {
                    return null;
                }
                else {
                    return record;
                }
            }
            else {
                if (ori < val) {
                    return null;
                }
                else {
                    return record;
                }
            }
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (hasEqual) {
                if (ori <= val) {
                    return null;
                }
                else {
                    return record;
                }
            }
            else {
                if (ori < val) {
                    return null;
                }
                else {
                    return record;
                }
            }
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            String ori = column.asString();
            if (hasEqual) {
                if (ori.compareTo(value) <= 0) {
                    return null;
                }
                else {
                    return record;
                }
            }
            else {
                if (ori.compareTo(value) < 0) {
                    return null;
                }
                else {
                    return record;
                }
            }
        }
        else {
            throw new RuntimeException("<=,< can't support this columnType:"
                    + column.getClass().getSimpleName());
        }
    }

    /**
     * DateColumn将比较long值，StringColumn，ByteColumn以及BooleanColumn比较其String值
     * @param record message record
     * @param value value to compared
     * @param column the column of record
     * @return Record
     */
    private Record doEqual(Record record, String value, Column column)
    {

        //如果字段为空，只比较目标字段为"null"，否则null字段均不过滤
        if (column.getRawData() == null) {
            if ("null".equalsIgnoreCase(value)) {
                return null;
            }
            else {
                return record;
            }
        }

        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (ori == val) {
                return null;
            }
            else {
                return record;
            }
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (ori == val) {
                return null;
            }
            else {
                return record;
            }
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            String ori = column.asString();
            if (ori.compareTo(value) == 0) {
                return null;
            }
            else {
                return record;
            }
        }
        else {
            throw new RuntimeException("== can't support this columnType:"
                    + column.getClass().getSimpleName());
        }
    }

    /**
     * DateColumn将比较long值，StringColumn，ByteColumn以及BooleanColumn比较其String值
     *
     * @param record message record
     * @param value value to compared
     * @param column the column of record
     * @return Record
     */
    private Record doNotEqual(Record record, String value, Column column)
    {

        //如果字段为空，只比较目标字段为"null", 否则null字段均过滤。
        if (column.getRawData() == null) {
            if ("null".equalsIgnoreCase(value)) {
                return record;
            }
            else {
                return null;
            }
        }

        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (ori != val) {
                return null;
            }
            else {
                return record;
            }
        }
        else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (ori != val) {
                return null;
            }
            else {
                return record;
            }
        }
        else if (column instanceof StringColumn
                || column instanceof BytesColumn
                || column instanceof BoolColumn) {
            String ori = column.asString();
            if (ori.compareTo(value) != 0) {
                return null;
            }
            else {
                return record;
            }
        }
        else {
            throw new RuntimeException("== can't support this columnType:"
                    + column.getClass().getSimpleName());
        }
    }

    private Record doLike(Record record, String value, Column column)
    {
        String orivalue = column.asString();
        if (orivalue != null && orivalue.matches(value)) {
            return null;
        }
        else {
            return record;
        }
    }

    private Record doNotLike(Record record, String value, Column column)
    {
        String orivalue = column.asString();
        if (orivalue != null && orivalue.matches(value)) {
            return record;
        }
        else {
            return null;
        }
    }
}
