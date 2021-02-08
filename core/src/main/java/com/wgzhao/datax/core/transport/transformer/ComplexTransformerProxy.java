package com.wgzhao.datax.core.transport.transformer;

import com.wgzhao.datax.common.element.Record;
import com.wgzhao.datax.transformer.ComplexTransformer;
import com.wgzhao.datax.transformer.Transformer;

import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/8.
 */
public class ComplexTransformerProxy
        extends ComplexTransformer
{
    private final Transformer realTransformer;

    public ComplexTransformerProxy(Transformer transformer)
    {
        setTransformerName(transformer.getTransformerName());
        this.realTransformer = transformer;
    }

    @Override
    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras)
    {
        return this.realTransformer.evaluate(record, paras);
    }

    public Transformer getRealTransformer()
    {
        return realTransformer;
    }
}
