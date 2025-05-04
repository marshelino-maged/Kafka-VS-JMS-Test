package com;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.*;
import java.util.concurrent.*;

public class LatencyMeasure {

    static final String BROKER_URL = "tcp://localhost:61616";
    static final String QUEUE_NAME = "latency";
    static final int TOTAL_MESSAGES = 10000;

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<List<Long>> futureLatencies = executor.submit(new Callable<List<Long>>() {
            public List<Long> call() {
                return consumeMessages();
            }
        });

        Thread.sleep(1000);

        produceMessages();

        List<Long> latencies = futureLatencies.get();

        executor.shutdown();

        Collections.sort(latencies);
        long medianNs = latencies.get(latencies.size() / 2);
        System.out.printf("Median Latency: %.3f ms%n", medianNs / 1_000_000.0);
    }

    static void produceMessages() {
        Connection connection = null;
        Session session = null;

        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE_NAME);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            for (int i = 0; i < TOTAL_MESSAGES; i++) {
                long sendTime = System.nanoTime();
                TextMessage message = session.createTextMessage("Message " + i);
                message.setLongProperty("sendTime", sendTime);
                producer.send(message);
            }

            System.out.println(TOTAL_MESSAGES + " is produced.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (session != null) session.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static List<Long> consumeMessages() {
        List<Long> latencies = new ArrayList<>();
        Connection connection = null;
        Session session = null;

        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(destination);

            for (int i = 0; i < TOTAL_MESSAGES; i++) {
                Message msg = consumer.receive(5000);
                if (msg != null && msg instanceof TextMessage) {
                    long sendTime = msg.getLongProperty("sendTime");
                    long latency = System.nanoTime() - sendTime;
                    latencies.add(latency);
                } else {
                    System.err.println("Message " + i + " timed out or was invalid.");
                }
            }

            System.out.println(TOTAL_MESSAGES + " is consumed.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (session != null) session.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return latencies;
    }
}
