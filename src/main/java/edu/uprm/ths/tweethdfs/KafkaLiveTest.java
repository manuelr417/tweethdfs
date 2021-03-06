package edu.uprm.ths.tweethdfs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by manuel on 2/19/17.
 */
public class KafkaLiveTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node05.ece.uprm.edu:9092");
        //props.put("group.id", "tweets2");
        props.put("group.id", "tweet-" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("trump"));
        Logger logger = LogManager.getRootLogger();
        logger.trace("Starting to get tweets");
        for (int i=0; i < 20; ++i) {
            logger.trace("Iteration: " + i);
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.trace("records: " + records.count());
                System.out.println(record.value());
                //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            }
        }
        consumer.close();
    }
}
