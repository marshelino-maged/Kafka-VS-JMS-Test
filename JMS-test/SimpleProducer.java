package com;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class SimpleProducer {
    static final String BROKER_URL = "tcp://localhost:61616";
    static final String QUEUE_NAME = "simple";
    static final String MESSAGE_CONTENT = "Simple Message";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = null;

        try {
            connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(QUEUE_NAME);
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage(MESSAGE_CONTENT);
            producer.send(message);
            System.out.println("Message sent.");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
