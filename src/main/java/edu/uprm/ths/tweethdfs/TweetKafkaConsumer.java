package edu.uprm.ths.tweethdfs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Exchanger;

/**
 * Created by manuel on 2/19/17.
 */
public class TweetKafkaConsumer<S, S1> {

    private HDFSHandler hdfsHandler;

    public static void main(String[] args) {
        Logger logger = LogManager.getRootLogger();
        logger.trace("Starting to trap tweets");
        Properties props = new Properties();
        props.put("bootstrap.servers", "node05.ece.uprm.edu:9092");
        //props.put("group.id", "tweets2");
        String groupId= UUID.randomUUID().toString();
        logger.trace("Group id: " + groupId);
        props.put("group.id", "tweet-" + groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        //consumer.subscribe(Arrays.asList("trump"));

        String topic = "trump";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        TopicPartition partition2 = new TopicPartition(topic, 2);

        consumer.assign(Arrays.asList(partition0, partition1, partition2));
        HDFSHandler handler = null;
        try {
            logger.trace("Creating handler");
            handler = HDFSHandler.init("hdfs://masternode.ece.uprm.edu:9000","/home/manuel/trump");
            logger.trace("Open the handler");
            handler.open();
            logger.trace("Getting the tweets");
            long i = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    handler.writeUTF(record.value());
                    ++i;
                }
                if ((i % 1000) ==0){
                    logger.trace(".");
                }
            }
        }
        catch(Exception e1){
            e1.printStackTrace();
            logger.trace("Exception: " + e1.toString());
        }
        finally {
            logger.trace("Something bad happened. Closing the handler");
            if (handler != null) {
                try {
                    handler.close();
                }
                catch (Exception e2){

                }
            }
            consumer.close();
        }

    }
}