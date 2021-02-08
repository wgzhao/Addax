package com.wgzhao.datax.core.transport.transformer;

import com.wgzhao.datax.common.element.Column;
import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.common.element.StringColumn;
import com.wgzhao.datax.common.exception.DataXException;
import com.wgzhao.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class PadTransformer
        extends Transformer
{
    public PadTransformer()
    {
        setTransformerName("dx_pad");
    }

    @Override
    public Record evaluate(Record record, Object... paras)
    {

        int columnIndex;
        String padType;
        int length;
        String padString;

        try {
            if (paras.length != 4) {
                throw new RuntimeException("dx_pad paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            padType = (String) paras[1];
            length = Integer.parseInt((String) paras[2]);
            padString = (String) paras[3];
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                    "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            //如果字段为空，作为空字符串处理
            if (oriValue == null) {
                oriValue = "";
            }
            String newValue;
            if (!"r".equalsIgnoreCase(padType) && !"l".equalsIgnoreCase(padType)) {
                throw new RuntimeException(String.format("dx_pad first para(%s) support l or r", padType));
            }
            if (length <= oriValue.length()) {
                newValue = oriValue.substring(0, length);
            }
            else {

                newValue = doPad(padType, oriValue, length, padString);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));
        }
        catch (Exception e) {
            throw DataXException.asDataXException(
                    TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }

    private String doPad(String padType, String oriValue, int length, String padString)
    {

        StringBuilder finalPad = new StringBuilder();
        int needLength = length - oriValue.length();
        while (needLength > 0) {

            if (needLength >= padString.length()) {
                finalPad.append(padString);
                needLength -= padString.length();
            }
            else {
                finalPad.append(padString, 0, needLength);
                needLength = 0;
            }
        }

        if ("l".equalsIgnoreCase(padType)) {
            return finalPad + oriValue;
        }
        else {
            return oriValue + finalPad;
        }
    }
}
