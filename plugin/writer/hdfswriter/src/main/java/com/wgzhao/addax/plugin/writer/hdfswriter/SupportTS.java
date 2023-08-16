package com.wgzhao.addax.plugin.writer.hdfswriter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.parquet.Version;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.JulianFields;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleToIntFunction;

import static java.lang.Math.pow;
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

    public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
        /*
         * Precision <= 18 checks for the max number of digits for an unscaled long,
         * else treat with big integer conversion
         */
        if (precision <= 18) {
            ByteBuffer buffer = value.toByteBuffer();
            byte[] bytes = buffer.array();
            int start = buffer.arrayOffset() + buffer.position();
            int end = buffer.arrayOffset() + buffer.limit();
            long unscaled = 0L;
            int i = start;
            while ( i < end ) {
                unscaled = ( unscaled << 8 | bytes[i] & 0xff );
                i++;
            }
            int bits = 8*(end - start);
            long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
            if (unscaledNew <= -pow(10,18) || unscaledNew >= pow(10,18)) {
                return new BigDecimal(unscaledNew);
            } else {
                return BigDecimal.valueOf(unscaledNew / pow(10,scale));
            }
        } else {
            return new BigDecimal(new BigInteger(value.getBytes()), scale);
        }
    }

    private static Binary decimalToBinary(String bigDecimal) {
        BigDecimal myDecimalValue = new BigDecimal(bigDecimal);
//First we need to make sure the BigDecimal matches our schema scale:
//        myDecimalValue = myDecimalValue.setScale(4, RoundingMode.HALF_UP);

//Next we get the decimal value as one BigInteger (like there was no decimal point)
        BigInteger myUnscaledDecimalValue = myDecimalValue.unscaledValue();

//Finally we serialize the integer
        byte[] decimalBytes = myUnscaledDecimalValue.toByteArray();

//We need to create an Avro 'Fixed' type and pass the decimal schema once more here:
//        GenericData.Fixed fixed = new GenericData.Fixed(
//                new Schema.Parser().parse("required fixed_len_byte_array(12) dec_field (DECIMAL(12,4));"));

        byte[] myDecimalBuffer = new byte[16];
        if (myDecimalBuffer.length >= decimalBytes.length) {
            //Because we set our fixed byte array size as 16 bytes, we need to
            //pad-left our original value's bytes with zeros
            int myDecimalBufferIndex = myDecimalBuffer.length - 1;
            for(int i = decimalBytes.length - 1; i >= 0; i--){
                myDecimalBuffer[myDecimalBufferIndex] = decimalBytes[i];
                myDecimalBufferIndex--;
            }
            return Binary.fromConstantByteArray(myDecimalBuffer);
            //Save result
//            fixed.bytes(myDecimalBuffer);
        } else {
            throw new IllegalArgumentException(String.format("Decimal size: %d was greater than the allowed max: %d", decimalBytes.length, myDecimalBuffer.length));
        }
        //We can finally write our decimal to our record

    }

    public static void main(String[] args) throws Exception {

        Binary binDec = decimalToBinary("12345.1234");
        System.out.println(binDec);
        BigDecimal bigDec = binaryToDecimal(binDec, 38, 4);;
        System.out.println(bigDec);
        Configuration conf = new Configuration();
        Path root = new Path("/tmp/parquet-test");
        FileUtils.deleteDirectory(new File("/tmp/parquet-test"));
        MessageType schema = parseMessageType(
                "message test { "
                        + "required binary binary_field; "
                        + "required int32 int32_field; "
                        + "required int32 birth_field (DATE); "
                        + "required int64 int64_field; "
                        + "required boolean boolean_field; "
                        + "required float float_field; "
                        + "required double double_field; "
                        + "required fixed_len_byte_array(3) flba_field; "
                        + "required int96 int96_field; "
                        + "required fixed_len_byte_array(16) dec_field (DECIMAL(38,10));"
                        + "} ");

        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        Path file = new Path(root, "parquet_1_0");
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(new TestOutputFile(file, conf))
                .withCompressionCodec(UNCOMPRESSED)
                .withPageSize(1024)
                .withDictionaryPageSize(512)
                .enableDictionaryEncoding()
                .withValidation(false)
                .withWriterVersion(PARQUET_1_0)
                .withDictionaryEncoding(false)
                .withConf(conf)
                .build();
        for (int i = 0; i < 10; i++) {
            writer.write(
                    f.newGroup()
                            .append("binary_field", "test1")
                            .append("int32_field", 32)
                            .append("birth_field", (int) LocalDate.now().toEpochDay()) //0x0e81
                            .append("int64_field", 64l)
                            .append("boolean_field", true)
                            .append("float_field", 1.0f)
                            .append("double_field", 2.0d)
                            .append("flba_field", "foo")
                            .append("int96_field", toInt96())
                            .append("dec_field", decimalToBinary("12345.1234"))
            );
        }
        writer.close();


    }
}

