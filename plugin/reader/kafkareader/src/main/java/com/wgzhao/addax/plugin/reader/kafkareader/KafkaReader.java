package com.wgzhao.addax.plugin.reader.kafkareader;

import com.alibaba.fastjson2.JSONObject;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.DateColumn;
import com.wgzhao.addax.common.element.DoubleColumn;
import com.wgzhao.addax.common.element.LongColumn;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.element.StringColumn;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordSender;
import com.wgzhao.addax.common.spi.Reader;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.wgzhao.addax.common.exception.CommonErrorCode.CONFIG_ERROR;
import static com.wgzhao.addax.common.exception.CommonErrorCode.REQUIRED_VALUE;

public class KafkaReader
        extends Reader
{
    public static class Job
            extends Reader.Job
    {
        private Configuration conf = null;

        @Override
        public void init()
        {
            this.conf = getPluginJobConf();
            conf.getNecessaryValue(KafkaKey.BROKER_LIST, REQUIRED_VALUE);
            conf.getNecessaryValue(KafkaKey.TOPIC, REQUIRED_VALUE);
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public List<Configuration> split(int adviceNumber)
        {
            // only one split
            return Collections.singletonList(conf.clone());
        }
    }

    public static class Task
            extends Reader.Task
    {
        private final static Logger logger = LoggerFactory.getLogger(Task.class);
        private final static String GROUP_ID = "addax-kafka-grp";
        private final static String CLIENT_ID = "addax-kafka-reader";

        Configuration configuration;
        KafkaConsumer<String, Object> kafkaConsumer;
        private List<String> columns;
        private String missKeyValue;

        @Override
        public void init()
        {
            this.configuration = getPluginJobConf();
            Properties properties = new Properties();
            String brokeLists = configuration.getString(KafkaKey.BROKER_LIST);
            String topic = configuration.getString(KafkaKey.TOPIC);
            this.columns = configuration.getList(KafkaKey.COLUMN, String.class);
            this.missKeyValue = configuration.getString(KafkaKey.MISSING_KEY_VALUE, null);
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokeLists);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
            Map<String, Object> custConf = this.configuration.getMap(KafkaKey.PROPERTIES);
            if (custConf != null && !custConf.isEmpty()) {
                properties.putAll(custConf);
            }
            this.kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            try {
                while (true) {
                    ConsumerRecords<String, Object> items = kafkaConsumer.poll(Duration.ofSeconds(100));
                    for (ConsumerRecord<String, Object> item : items) {
                        Record record = recordSender.createRecord();
                        logger.debug("topic = {}, partition = {}, offset = {}, kafkaConsumer = {}, country = {}%n",
                                item.topic(), item.partition(), item.offset(),
                                item.key(), item.value());
                        final JSONObject jsonObject = JSONObject.parseObject(item.value().toString());
                        if (columns.size() == 1 && "*".equals(columns.get(0))) {
                            //assume all json value type is string
                            for (String key : jsonObject.keySet()) {
                                record.addColumn(new StringColumn(jsonObject.getString(key)));
                            }
                        }
                        else {
                            for (String col : columns) {
                                if (!jsonObject.containsKey(col)) {
                                    if (this.missKeyValue == null) {
                                        throw AddaxException.asAddaxException(CONFIG_ERROR,
                                                "The column " + col + " not exists");
                                    }
                                    record.addColumn(new StringColumn(this.missKeyValue));
                                }
                                else {
                                    record.addColumn(guessColumnType(jsonObject.get(col)));
                                }
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                }
            }
            finally {
                kafkaConsumer.close();
            }
        }

        private Column guessColumnType(Object obj)
        {
            if (obj instanceof Long) {
                return new LongColumn((Long) obj);
            }
            if (obj instanceof Date) {
                return new DateColumn((Date) obj);
            }
            if (obj instanceof Double) {
                return new DoubleColumn((Double) obj);
            }
            return new StringColumn(obj.toString());
        }
    }
}
