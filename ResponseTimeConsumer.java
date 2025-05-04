package com;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ResponseTimeConsumer {
    static final String BROKER_URL = "tcp://localhost:61616";
    static final String QUEUE_NAME = "responseTime";
    static final String MESSAGE_PATH = "message.txt";
    static final int TOTAL_MESSAGES = 1000;

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = null;

        try {
            connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            String content = new String(Files.readAllBytes(Paths.get(MESSAGE_PATH)));

            int falseMessages = 0;
            long totalTime = 0;
            List<Long> responseTimes = new ArrayList<>();
            for (int i = 0; i < TOTAL_MESSAGES; i++) {
                long start = System.currentTimeMillis();
                Message message = consumer.receive();
                long responseTime = System.currentTimeMillis() - start;

                totalTime += responseTime;
                responseTimes.add(responseTime);

                String messageContent = ((TextMessage) message).getText();
                if (!content.equals(messageContent)) {
                    falseMessages++;
                }
            }

            Collections.sort(responseTimes);
            long median;
            median = (responseTimes.get(TOTAL_MESSAGES / 2) + responseTimes.get(TOTAL_MESSAGES / 2 - (TOTAL_MESSAGES % 2 == 0 ? 1 : 0))) / 2;

            System.out.println("Number of false messages: " + falseMessages);
            System.out.println("Total consume time: " + totalTime + " ms");
            System.out.println("Median consume time: " + median + " ms");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
