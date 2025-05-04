package com;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.google.common.util.concurrent.RateLimiter;

import java.nio.file.Files;
import java.nio.file.Paths;

public class ThroughputProducer {
    static final String BROKER_URL = "tcp://localhost:61616";
    static final String QUEUE_NAME = "throughput";
    static final String MESSAGE_PATH = "message.txt";
    static final int MESSAGES_PER_SECOND = 10000000;
    static final int TOTAL_MESSAGES = 10000000;

    public static void main(String[] args) {
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

            RateLimiter limiter = RateLimiter.create(MESSAGES_PER_SECOND);

            long startTime = System.currentTimeMillis();
            for (int i = 0; i < TOTAL_MESSAGES; i++) {
                limiter.acquire();

                String content = new String(Files.readAllBytes(Paths.get(MESSAGE_PATH)));
                TextMessage message = session.createTextMessage(content);
                producer.send(message);
            }
            long totalTime = System.currentTimeMillis() - startTime;

            System.out.println(TOTAL_MESSAGES + " messages produced in " + totalTime / 1000.0 + " seconds");
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
}
