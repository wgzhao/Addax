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

package com.wgzhao.datax.plugin.writer.hdfswriter;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

public class TestParquetFile
{
    private static String filePath = "/tmp/datax_test.parquet";
    Schema.Parser parser = new Schema.Parser().setValidate(true);
    CompressionCodecName compress = CompressionCodecName.SNAPPY;
    Schema schema = null;

    public TestParquetFile()
            throws IOException
    {
        schema = parser.parse(TestParquetFile.class.getResourceAsStream("/parquet_schema.asvo"));
    }

    @Test
    public void testParquetWrite()
    {
        File file = new File(filePath);
        file.delete();
//        System.out.println("file schema = " + schema.toString(true));
        // add the decimal conversion to a generic data model
        GenericData decimalSupport = new GenericData();
        decimalSupport.addLogicalTypeConversion(new Conversions.DecimalConversion());
        Path path = new Path(file.toString());
        try (ParquetWriter<GenericRecord> writer =  AvroParquetWriter
                .<GenericRecord>builder(path)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withSchema(schema)
                .withConf(new Configuration())
                .withValidation(false)
                .withDictionaryEncoding(false)
                .withDataModel(decimalSupport)
                .withCompressionCodec(compress)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .build()
        ){
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("col1", Long.MAX_VALUE - 1);
            builder.set("col2", Integer.MAX_VALUE - 1);
            builder.set("col3", "this is parquet test file");
            BigDecimal dec = new BigDecimal("123456789.123456").setScale(10, BigDecimal.ROUND_HALF_UP);
            builder.set("col4", dec);
            java.sql.Date date = new java.sql.Date(Timestamp.valueOf("1989-06-04 13:14:15").getTime());
            builder.set("col5", date.getTime() / 1000);
            builder.set("col6", Timestamp.valueOf("1989-06-04 13:14:15").getTime()/ 1000);
            writer.write(builder.build());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
