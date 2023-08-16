package com.wgzhao.addax.plugin.writer.hdfswriter;

import org.apache.avro.LogicalTypes;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.JulianFields;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleToIntFunction;

import static java.util.Arrays.asList;
import static org.apache.parquet.column.Encoding.*;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;


public class SupportTS {

    private static class TestOutputFile implements OutputFile {

        private final OutputFile outputFile;

        TestOutputFile(Path path, Configuration conf) throws IOException {
            outputFile = HadoopOutputFile.fromPath(path, conf);
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return outputFile.create(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return outputFile.createOrOverwrite(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return outputFile.supportsBlockSize();
        }

        @Override
        public long defaultBlockSize() {
            return outputFile.defaultBlockSize();
        }
    }

    private static Binary toInt96() throws ParseException {
        String value = "2019-02-13 13:35:05";

        final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
        final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
        final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

// Parse date
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(parser.parse(value));

// Calculate Julian days and nanoseconds in the day
        LocalDate dt = LocalDate.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
        int julianDays = (int) JulianFields.JULIAN_DAY.getFrom(dt);
        long nanos = (cal.get(Calendar.HOUR_OF_DAY) * NANOS_PER_HOUR)
                + (cal.get(Calendar.MINUTE) * NANOS_PER_MINUTE)
                + (cal.get(Calendar.SECOND) * NANOS_PER_SECOND);

// Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(julianDays);

// This is the properly encoded INT96 timestamp
        Binary tsValue = Binary.fromReusedByteArray(timestampBuffer);
        return tsValue;
    }

    private static Binary decimalToBinary(final BigDecimal bigDecimal) {
        // Map precision to the number bytes needed for binary conversion.
        HiveDecimal hiveDecimal  = HiveDecimal.create(bigDecimal);
        int PRECISION_TO_BYTE_COUNT[] = new int[38];

            for (int prec = 1; prec <= 38; prec++) {
                // Estimated number of bytes needed.
                PRECISION_TO_BYTE_COUNT[prec - 1] = (int)
                        Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
            }

        int prec = bigDecimal.precision();
        int scale = bigDecimal.scale();
        byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);

        // Estimated number of bytes needed.
        int precToBytes = PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }

        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
        return Binary.fromByteArray(tgt);

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path root = new Path("/tmp/parquet-test");
        FileUtils.deleteDirectory(new File("/tmp/parquet-test"));
        MessageType schema = parseMessageType(
                "message test { "
                        + "required binary binary_field; "
                        + "required int32 int32_field; "
                        + "required int64 int64_field; "
                        + "required boolean boolean_field; "
                        + "required float float_field; "
                        + "required double double_field; "
                        + "required fixed_len_byte_array(3) flba_field; "
                        + "required fixed_len_byte_array(6) dec_field(DECIMAL(12,4));"
                        + "required int96 int96_field; "
                        + "} ");

        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        Map<String, Encoding> expected = new HashMap<String, Encoding>();
        expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
        expected.put("1000-" + PARQUET_1_0, PLAIN);
        expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
        expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
//        BigDecimal bigDecimal = new BigDecimal("12.345").setScale(3, RoundingMode.UP);

        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path("/tmp/000000_0")).withConf(conf).build();
        MessageType schema1 = readFooter(conf, new Path("/tmp/000000_0")).getFileMetaData().getSchema();
        System.out.println(schema1.toString());

        for (int modulo : asList(10, 1000)) {
            for (WriterVersion version : WriterVersion.values()) {
                Path file = new Path(root, version.name() + "_" + modulo);
                ParquetWriter<Group> writer = ExampleParquetWriter.builder(new TestOutputFile(file, conf))
                        .withCompressionCodec(UNCOMPRESSED)
                        .withRowGroupSize(1024)
                        .withPageSize(1024)
                        .withDictionaryPageSize(512)
                        .enableDictionaryEncoding()
                        .withValidation(false)
                        .withWriterVersion(version)
                        .withConf(conf)
                        .build();
                for (int i = 0; i < 1000; i++) {
                    writer.write(
                            f.newGroup()
                                    .append("binary_field", "test" + (i % modulo))
                                    .append("int32_field", 32)
                                    .append("int64_field", 64l)
                                    .append("boolean_field", true)
                                    .append("float_field", 1.0f)
                                    .append("double_field", 2.0d)
                                    .append("flba_field", "foo")
                                    .append("int96_field", toInt96())
                                    .append("dec_field", decimalToBinary(new BigDecimal("12.345")))
                    );
                }
                writer.close();
//                ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
//                for (int i = 0; i < 1000; i++) {
//                    Group group = reader.read();
//                    assertEquals("test" + (i % modulo), group.getBinary("binary_field", 0).toStringUsingUTF8());
//                    assertEquals(32, group.getInteger("int32_field", 0));
//                    assertEquals(64l, group.getLong("int64_field", 0));
//                    assertEquals(true, group.getBoolean("boolean_field", 0));
//                    assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
//                    assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
//                    assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
//                    assertEquals(toInt96(), group.getInt96("int96_field", 0));
//                }
//                reader.close();
//                ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
//                for (BlockMetaData blockMetaData : footer.getBlocks()) {
//                    for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
//                        if (column.getPath().toDotString().equals("binary_field")) {
//                            String key = modulo + "-" + version;
//                            Encoding expectedEncoding = expected.get(key);
//                            assertTrue(
//                                    key + ":" + column.getEncodings() + " should contain " + expectedEncoding,
//                                    column.getEncodings().contains(expectedEncoding));
//                        }
//                    }
//                }
//                assertEquals("Object model property should be example",
//                        "example", footer.getFileMetaData().getKeyValueMetaData()
//                                .get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
            }
        }
    }
}

