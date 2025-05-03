package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Producer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        String bootstrapServer = "localhost:9092";
        String topicName = "test-topic";
        String message;

        try {
            message = new String(Files.readAllBytes(Paths.get("message.txt")));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);

        ArrayList<Long> times = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            // Send data synchronously
            long start = System.currentTimeMillis();
            producer.send(record).get();
            long end = System.currentTimeMillis();
            times.add(end - start);
        }

        producer.flush();
        producer.close();

        // Calculate and print the median time taken
        times.sort(Long::compareTo);
        long medianTime;
        if (times.size() % 2 == 0) {
            medianTime = (times.get(times.size() / 2 - 1) + times.get(times.size() / 2)) / 2;
        } else {
            medianTime = times.get(times.size() / 2);
        }
        System.out.println("Median time taken to send message: " + medianTime + " ms");
    }
}
