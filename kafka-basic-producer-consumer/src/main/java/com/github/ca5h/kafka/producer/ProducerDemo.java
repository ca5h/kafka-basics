package com.github.ca5h.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private final String topic = "first_topic";
    private final String kafkaServer = "127.0.0.1:9092";

    public ProducerDemo() {
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public void run() {
        KafkaProducer<String, String> producer = createKafkaProducer();

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello world");

        //send data
        producer.send(record);

        //flush
        producer.flush();

        //close
        producer.close();
    }


    public static void main(String[] args) {
        new ProducerDemo().run();
    }

}
