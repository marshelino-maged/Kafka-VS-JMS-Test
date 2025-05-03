package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Latency {
    public static void main(String[] args) {
        Thread consumerThread = new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test-group13");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("test-topic"));

            int pollCount = 0;
            ArrayList<Long> times = new ArrayList<>();

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    long end = System.currentTimeMillis();
                    for (ConsumerRecord<String, String> record : records) {
                        long start = record.timestamp();
                        times.add(end - start);
                        pollCount++;
                    }
                    // System.out.println("poll count: " + pollCount);
                    if (pollCount >= 10000) {
                        break;
                    }
                }
                // Calculate and print the median time taken
                times.sort(Long::compareTo);
                long medianTime;
                if (times.size() % 2 == 0) {
                    medianTime = (times.get(times.size() / 2 - 1) + times.get(times.size() / 2)) / 2;
                } else {
                    medianTime = times.get(times.size() / 2);
                }
                System.out.println("Median Latency Time: " + medianTime + " ms");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        });

        Thread producerThread = new Thread(() -> {

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            String message;

            try {
                message = new String(Files.readAllBytes(Paths.get("message.txt")));
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            for (int i = 0; i <= 50000; i++) {
                long start = System.currentTimeMillis();
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", null, start, null, message);
                try {
                    producer.send(record).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            producer.flush();
            producer.close();
        });

        producerThread.start();
        consumerThread.start();
    }
}
