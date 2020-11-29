package com.github.ca5h.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {

    private final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    private final String topic = "first_topic";
    private final String kafkaServer = "127.0.0.1:9092";

    public ProducerDemoWithCallback() {
    }

    public void run()  throws ExecutionException, InterruptedException{
        KafkaProducer<String, String> producer = createKafkaProducer();

        for (int i = 0; i < 10; i++) {
            //create producer record
            String value = "hello world " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key);

            //send data async
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n");
                } else {
                    logger.error("Error while producing", e);
                }
            }).get();

        }

        //flush
        producer.flush();

        //close
        producer.close();
    }

    private  KafkaProducer<String, String> createKafkaProducer() {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        try {
            new ProducerDemoWithCallback().run();
        } catch (ExecutionException | InterruptedException e ) {
            e.printStackTrace();
        }
    }

}
