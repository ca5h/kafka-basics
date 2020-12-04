package com.github.ca5h.kafka.elastic;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
        return  jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        //Create elastic client
        RestHighLevelClient client = ElasticClientProvider.createElasticClient();

        //Create Kafka consumer
        KafkaConsumer<String,String> consumer = KafkaConsumerProvider.createKafkaConsumer();
        consumer.subscribe(Arrays.asList("twitter_tweets"));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int recordCount = records.count();

            logger.info("Received " + recordCount + " records" );

            BulkRequest bulkRequest = new BulkRequest();

            records.forEach(record -> {
                String id = extractIdFromTweet(record.value());
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id) //make consumer idempotent
                                                .source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);

            });
            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();

    }

}
