package com.github.ca5h.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    private final String topic = "first_topic";
    private final String groupId = "test-group-id";
    private final String kafkaServer = "127.0.0.1:9092";

    public ConsumerDemo() {
    }

    public  void run() {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                logger.info("Key: {} , Value: {}, Partition: {}, Offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
            });
        }
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        return new KafkaConsumer<>(properties);
    }


    public static void main(String[] args) {
        new ConsumerDemo().run();
    }
}
