/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.addax.plugin.writer.kafkawriter;

import com.alibaba.fastjson.JSON;
import com.wgzhao.addax.common.element.Column;
import com.wgzhao.addax.common.element.Record;
import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.plugin.RecordReceiver;
import com.wgzhao.addax.common.spi.Writer;
import com.wgzhao.addax.common.util.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.wgzhao.addax.common.base.Constant.DEFAULT_BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.BATCH_SIZE;
import static com.wgzhao.addax.common.base.Key.COLUMN;
import static com.wgzhao.addax.plugin.writer.kafkawriter.KafkaKey.BROKER_LIST;
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
            config.getNecessaryValue(BROKER_LIST, KafkaWriterErrorCode.REQUIRED_VALUE);
            config.getNecessaryValue(TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
            List<String> columns = config.getList(COLUMN, String.class);
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

        @Override
        public void init()
        {
            this.configuration = getPluginJobConf();
            String brokeLists = configuration.getString(BROKER_LIST);
            this.topic = configuration.getString(TOPIC);
            this.columns = configuration.getList(COLUMN, String.class);

            // try to connect to kafka
            Map<String, Object> propMap = configuration.getMap(PROPERTIES);

            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokeLists);
            kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
            kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, configuration.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE));
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            if (propMap != null) {
                propMap.forEach((k, v) -> kafkaProps.setProperty(k, v.toString()));
            }
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
                Map<String, Object> message = new HashMap<>();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    String key = columns.get(i);
                    switch (column.getType()) {
                        case INT:
                        case LONG:
                            message.put(key, column.asLong());
                            break;
                        case DOUBLE:
                            message.put(key, column.asDouble());
                            break;
                        case BOOL:
                            message.put(key, column.asBoolean());
                            break;
                        case DATE:
                            message.put(key, column.asDate());
                            break;
                        case BYTES:
                            message.put(key, column.asBytes());
                            break;
                        case TIMESTAMP:
                            message.put(key, column.asTimestamp());
                            break;
                        default:
                            message.put(key, column.asString());
                            break;
                    }
                }
                // convert java map to json string
                String json = JSON.toJSONString(message);

                producerRecord = new ProducerRecord<>(topic, json);

                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        throw AddaxException.asAddaxException(KafkaWriterErrorCode.PUT_KAFKA_ERROR, exception);
                    }
                });
            }
        }
    }
}
