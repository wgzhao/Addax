/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.hdfswriter;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TestOrcFile
{
    private static String filePath = "/tmp/my-file.orc";

    public void TestOrcWrite()
            throws IOException
    {
        TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string,z:timestamp,b:binary,d:decimal(38,12)>");
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
        byte[] image = IOUtils.toByteArray(TestOrcFile.class.getResourceAsStream("/datax_logo.png"));

        Writer writer = OrcFile.createWriter(new Path(filePath),
                OrcFile.writerOptions(new Configuration()).setSchema(schema));
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector x = (LongColumnVector)  batch.cols[0];
        BytesColumnVector y = (BytesColumnVector) batch.cols[1];
        TimestampColumnVector z = (TimestampColumnVector) batch.cols[2];
        BytesColumnVector b = (BytesColumnVector) batch.cols[3];
        DecimalColumnVector d = (DecimalColumnVector) batch.cols[4];
        x.noNulls = false;
        y.noNulls = false;
        z.noNulls = false;
        b.noNulls = false;
        d.noNulls = false;
        int row ;

        // test non-null data
        row = batch.size++;
        x.vector[row] = 1;
        y.setVal(row, "hello".getBytes(StandardCharsets.UTF_8));
        z.set(row, Timestamp.valueOf("2020-10-12 17:15:14"));
        b.setRef(row, image, 0, image.length);
        // test long type with null
        row = batch.size++;
        x.isNull[row] = true;
        y.setVal(row,"world".getBytes(StandardCharsets.UTF_8));
        z.set(row, Timestamp.valueOf("2021-01-13 12:15:01"));
        b.setRef(row, image, 0, image.length);
        // test string type with null
        row = batch.size++;
        x.vector[row] = 3;
        y.isNull[row] = true;
        z.set(row, Timestamp.valueOf("2021-06-13 01:01:22"));
        b.setRef(row, image, 0, image.length);
        // test timestamp type with null
        row = batch.size++;
        x.vector[row] = 4;
        y.setVal(row, "跨越防火墙".getBytes(StandardCharsets.UTF_8));
        z.isNull[row] = true;
        b.setRef(row, image, 0, image.length);

        row = batch.size++;
        x.vector[row] = 5;
        y.setVal(row, "奴役即自由".getBytes(StandardCharsets.UTF_8));
        z.set(row, Timestamp.valueOf("2021-07-02 09:12:15"));
        b.isNull[row] = true;
        HiveDecimalWritable hdw = new HiveDecimalWritable();
        hdw.set(HiveDecimal.create("1234567891234567.123456789156").setScale(10, HiveDecimal.ROUND_HALF_UP));
//        hdw.setFromDouble(Double.parseDouble("1234567891234567.123456789111"));
        d.set(row, hdw);
        writer.addRowBatch(batch);
        writer.close();
    }

    @Test
    public void testOrcReader()
            throws IOException
    {
        TestOrcWrite();
        Reader reader = OrcFile.createReader(new Path(filePath), OrcFile.readerOptions(new Configuration()));
        System.out.println("File schema: " + reader.getSchema());
        System.out.println("Row count: " + reader.getNumberOfRows());

        TypeDescription schema = reader.getSchema();
        System.out.println(schema.getChildren().get(2).getCategory());
        List<String> fieldNames = schema.getFieldNames();
        VectorizedRowBatch batch = schema.createRowBatch();
        RecordReader rows = reader.rows(reader.options().schema(schema));
        LongColumnVector x = (LongColumnVector)  batch.cols[0];
        BytesColumnVector y = (BytesColumnVector) batch.cols[1];
        TimestampColumnVector z = (TimestampColumnVector) batch.cols[2];
        BytesColumnVector b = (BytesColumnVector) batch.cols[3];
        DecimalColumnVector d = (DecimalColumnVector) batch.cols[4];
        for (String colName: fieldNames) {
            System.out.print(colName + "\t");
        }
        System.out.println();
        while (rows.nextBatch(batch)) {
            for (int row = 0; row < batch.size; row++) {
                Date date = z.isNull[row] ? null : new Date(z.getTime(row));
                byte[] val = Arrays.copyOfRange(b.vector[row], b.start[row], b.start[row] + b.length[row]);
                System.out.println(x.vector[row] + "\t" + y.toString(row)
                        + "\t" + date +  "\t" + d.vector[row] + "\t" + val.length);
            }
        }
    }
}
