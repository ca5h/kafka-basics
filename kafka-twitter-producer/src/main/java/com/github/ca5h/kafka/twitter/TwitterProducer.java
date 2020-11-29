package com.github.ca5h.kafka.twitter;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);


    public TwitterProducer() {
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        //create twitter client
        Client client = TwitterClientProvider.createTwitterClient(msgQueue);
        client.connect();

        //create kafka producer
        KafkaProducer<String, String> producer = KafkaProducerProvider.createKafkaProducer();

        //send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("", e);
                client.stop();
            }
            if(msg!= null){
                logger.info(msg);
                producer.send(new ProducerRecord("twitter_tweets", null, msg),
                (recordMetadata, e) -> {
                    if (e != null){
                        logger.error("Error occured", e);
                    }
                });
            }
        }
        logger.info("End of the application");
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

}
