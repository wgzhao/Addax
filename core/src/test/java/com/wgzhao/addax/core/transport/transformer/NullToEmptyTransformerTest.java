package com.wgzhao.addax.core.transport.transformer;

import com.wgzhao.addax.core.element.Column;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.element.DefaultRecord;
import com.wgzhao.addax.core.transport.record.DefaultRecordCreator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NullToEmptyTransformerTest {

    @Test
    public void testTransformAllColumns() {
        // Create a record with some null values
        Record record = DefaultRecordCreator.createRecord();
        record.addColumn(new StringColumn("value1"));
        record.addColumn(null);  // null column
        record.addColumn(new StringColumn(null));  // string with null raw data
        record.addColumn(new StringColumn("value4"));

        // Create transformer and evaluate
        NullToEmptyTransformer transformer = new NullToEmptyTransformer();
        Record result = transformer.evaluate(record);

        // Verify
        assertNotNull(result);
        assertEquals("value1", result.getColumn(0).asString());
        assertEquals("", result.getColumn(1).asString());  // null -> empty
        assertEquals("", result.getColumn(2).asString());  // null raw data -> empty
        assertEquals("value4", result.getColumn(3).asString());
    }

    @Test
    public void testTransformSpecificColumn() {
        // Create a record
        Record record = DefaultRecordCreator.createRecord();
        record.addColumn(new StringColumn("value1"));
        record.addColumn(null);  // should be transformed
        record.addColumn(new StringColumn("value3"));

        // Transform only column 1
        NullToEmptyTransformer transformer = new NullToEmptyTransformer();
        Record result = transformer.evaluate(record, 1);

        // Verify
        assertNotNull(result);
        assertEquals("value1", result.getColumn(0).asString());
        assertEquals("", result.getColumn(1).asString());  // transformed
        assertEquals("value3", result.getColumn(2).asString());
    }

    @Test
    public void testNoNullValues() {
        // Create a record with no null values
        Record record = DefaultRecordCreator.createRecord();
        record.addColumn(new StringColumn("value1"));
        record.addColumn(new StringColumn("value2"));

        // Transform
        NullToEmptyTransformer transformer = new NullToEmptyTransformer();
        Record result = transformer.evaluate(record);

        // Verify - should not change
        assertNotNull(result);
        assertEquals("value1", result.getColumn(0).asString());
        assertEquals("value2", result.getColumn(1).asString());
    }

    @Test
    public void testNullRecord() {
        NullToEmptyTransformer transformer = new NullToEmptyTransformer();
        Record result = transformer.evaluate(null);
        assertNull(result);
    }

    @Test
    public void testTransformerName() {
        NullToEmptyTransformer transformer = new NullToEmptyTransformer();
        assertEquals("dx_null_to_empty", transformer.getTransformerName());
    }
}
