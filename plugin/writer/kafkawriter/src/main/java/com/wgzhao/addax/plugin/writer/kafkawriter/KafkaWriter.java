package com.wgzhao.addax.plugin.writer.kafkawriter;

import com.wgzhao.addax.common.base.Key;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.plugin.writer.kafkawriter.KafkaKey.BROKER_LIST;
import static com.wgzhao.addax.plugin.writer.kafkawriter.KafkaKey.PARTITIONS;
import static com.wgzhao.addax.plugin.writer.kafkawriter.KafkaKey.PROPERTIES;
import static com.wgzhao.addax.plugin.writer.kafkawriter.KafkaKey.TOPIC;

public class KafkaWriter
        extends Writer
{
    public static class Job
            extends Writer.Job
    {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration config = null;

        @Override
        public void init()
        {
            this.config = this.getPluginJobConf();
            validateParameter();
        }

        private void validateParameter()
        {
            Configuration connection = config.getListConfiguration(Key.CONNECTION).get(0);
            connection.getNecessaryValue(BROKER_LIST, KafkaWriterErrorCode.REQUIRED_VALUE);
            connection.getNecessaryValue(TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
            List<String> columns = this.config.getList(COLUMN, String.class);
            if (columns == null || columns.isEmpty() || (columns.size() == 1 && Objects.equals(columns.get(0), "*"))) {
                throw AddaxException.asAddaxException(KafkaWriterErrorCode.ILLEGAL_VALUE, "the item column must be configure and be not " +
                        "'*'");
            }
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            // only one split
            return Collections.singletonList(config.clone());
        }
    }

    public static class Task
            extends Writer.Task
    {

        private final static String CLIENT_ID = "addax-kafka-writer";

        Configuration configuration;
        KafkaProducer<String, Object> kafkaProducer;
        private String topic;
        private List<String> columns;
        private int partitions;

        @Override
        public void init()
        {
            this.configuration = getPluginJobConf();
            Configuration connection = configuration.getListConfiguration(Key.CONNECTION).get(0);
            String brokeLists = connection.getString(BROKER_LIST);
            this.topic = connection.getString(TOPIC);
            this.columns = configuration.getList(COLUMN, String.class);

            this.partitions = connection.getInt(PARTITIONS, 3);

            // try to connect to kafka
            Map<String, Object> propMap = connection.getMap(PROPERTIES);

            Properties kafkaProps = new Properties();
            propMap.forEach((k, v) -> kafkaProps.setProperty(k, v.toString()));
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokeLists);
            kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
            kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, connection.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE));
            propMap.forEach((k, v) -> kafkaProps.setProperty(k, v.toString()));
            kafkaProducer = new KafkaProducer<>(kafkaProps);
        }

        @Override
        public void destroy()
        {
            //
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver)
        {
            Record record;
            ProducerRecord<String, Object> producerRecord;
            while ((record = lineReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != columns.size()) {
                    String msg = String.format("Your item column has %d , but the record has %d", columns.size(),
                            record.getColumnNumber());
                    throw AddaxException.asAddaxException(KafkaWriterErrorCode.NOT_MATCHED_COLUMNS, msg);
                }

                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    String key = columns.get(i);
                    switch (column.getType()) {
                        case INT:
                        case LONG:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asLong());
                            break;
                        case DOUBLE:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asDouble());
                            break;
                        case BOOL:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asBoolean());
                            break;
                        case DATE:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asDate());
                            break;
                        case BYTES:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asBytes());
                            break;
                        case TIMESTAMP:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asTimestamp());
                            break;
                        default:
                            producerRecord = new ProducerRecord<>(topic, partitions, key, column.asString());
                            break;
                    }
                    kafkaProducer.send(producerRecord);
                }
            }
        }
    }
}
