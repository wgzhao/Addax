package com.wgzhao.addax.core.transport.transformer;

import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.core.element.LongColumn;
import com.wgzhao.addax.core.element.Record;
import com.wgzhao.addax.core.element.StringColumn;
import com.wgzhao.addax.core.transport.record.DefaultRecord;

public class StringNullToEmptyTransformerTest {

    public static void main(String[] args) {
        System.out.println("=== Testing dx_string_null_to_empty ===\n");

        // Test 1: Transform string columns
        System.out.println("Test 1: Transform null string to empty");
        Record record1 = new DefaultRecord();
        record1.addColumn(new StringColumn(null));  // null string
        record1.addColumn(new LongColumn((Long) null));     // null long
        record1.addColumn(new StringColumn("value")); // non-null

        JSONObject param = new JSONObject();
        param.put("column", new Object[]{
            new JSONObject() {{ put("name", "str_col"); put("type", "string"); }},
            new JSONObject() {{ put("name", "long_col"); put("type", "long"); }},
            new JSONObject() {{ put("name", "str_col2"); put("type", "string"); }}
        });

        StringNullToEmptyTransformer transformer = new StringNullToEmptyTransformer();
        Record result1 = transformer.evaluate(record1, param.toJSONString());

        System.out.println("  Column 0 (string, null): " + result1.getColumn(0).asString());
        System.out.println("  Column 1 (long, null): " + result1.getColumn(1).getRawData());
        System.out.println("  Column 2 (string, 'value'): " + result1.getColumn(2).asString());

        boolean test1Pass = "".equals(result1.getColumn(0).asString())
            && result1.getColumn(1).getRawData() == null
            && "value".equals(result1.getColumn(2).asString());
        System.out.println("  Result: " + (test1Pass ? "PASS ✓" : "FAIL ✗"));
        System.out.println();

        // Test 2: No parameter - should not transform
        System.out.println("Test 2: No parameter - should not transform");
        Record record2 = new DefaultRecord();
        record2.addColumn(new StringColumn(null));

        Record result2 = transformer.evaluate(record2);
        System.out.println("  Column 0 (no param): " + result2.getColumn(0).getRawData());
        boolean test2Pass = result2.getColumn(0).getRawData() == null;
        System.out.println("  Result: " + (test2Pass ? "PASS ✓" : "FAIL ✗"));
        System.out.println();

        // Test 3: Null record
        System.out.println("Test 3: Null record");
        Record result3 = transformer.evaluate(null);
        boolean test3Pass = result3 == null;
        System.out.println("  Result: " + (test3Pass ? "PASS ✓" : "FAIL ✗"));
        System.out.println();

        // Test 4: Transformer name
        System.out.println("Test 4: Transformer name");
        String name = transformer.getTransformerName();
        System.out.println("  Name: " + name);
        boolean test4Pass = "dx_string_null_to_empty".equals(name);
        System.out.println("  Result: " + (test4Pass ? "PASS ✓" : "FAIL ✗"));

        System.out.println("\n=== All tests completed ===");
    }
}
