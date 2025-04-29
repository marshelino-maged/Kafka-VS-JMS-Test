package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String topicName = "test-topic";

        // need to change this name for each test
        String groupId = "test-group6";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topicName));

        consumer.poll(Duration.ofMillis(0));

        ArrayList<Long> times = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {

            consumer.seekToBeginning(consumer.assignment());

            int pollCount = 0;
            long start = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                pollCount += records.count();
                if (pollCount >= 1000) {
                    break;
                }
            }
            long end = System.currentTimeMillis();
            times.add(end - start);
            System.out.println("Time taken for poll: " + (end - start) + " ms");
        }

        consumer.close();

        // Calculate and print the median time taken
        times.sort(Long::compareTo);
        long medianTime;
        if (times.size() % 2 == 0) {
            medianTime = (times.get(times.size() / 2 - 1) + times.get(times.size() / 2)) / 2;
        } else {
            medianTime = times.get(times.size() / 2);
        }
        System.out.println("Median time taken for poll: " + medianTime + " ms");
    }
}
