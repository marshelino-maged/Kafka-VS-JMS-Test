package com;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class SimpleConsumer {
    static final String BROKER_URL = "tcp://localhost:61616";
    static final String QUEUE_NAME = "simple";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = null;

        try {
            connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive();
            System.out.println("Message received: " + ((TextMessage) message).getText());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
