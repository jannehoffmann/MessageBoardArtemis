package de.thm.mni.vs.gruppeF;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;

import java.util.concurrent.CountDownLatch;

public class Server {

    private static Session session;
    private static MessageProducer producerChannels;
    
    public static void main(String[] args) {
        String brokerUrl = "tcp://localhost:61616";
        Connection connection = null;
        ActiveMQConnectionFactory connectionFactory = null;
        // CountDownLatch um den Server am Laufen zu halten, bis er beendet wird
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        try {
            // Verbindung zum Broker herstellen
            connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection("artemis", "artemis");
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Producer für das "channels"-Topic erstellen, um neue Kanäle zu veröffentlichen
            Topic topicChannels = session.createTopic("channels");
            producerChannels = session.createProducer(topicChannels);

            // Consumer für das "channelRequests"-Topic erstellen, um neue Kanalanforderungen zu empfangen
            Topic topicChannelRequests = session.createTopic("channelRequests");
            MessageConsumer consumerChannelRequests = session.createConsumer(topicChannelRequests);
            consumerChannelRequests.setMessageListener(Server::onMessage);

            System.out.println("Server läuft. Beenden mit Strg+C.");

            // Warten, bis der Server beendet wird
            shutdownLatch.await();
        } catch (JMSException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connectionFactory != null) {
                connectionFactory.close();
            }
        }
    }

    // Methode, die aufgerufen wird, wenn eine neue Kanalanforderung empfangen wird
    public static void onMessage(Message message) {
        if (message instanceof TextMessage textMessage) {
            try {
                // Den Namen des angeforderten Kanals aus der Nachricht extrahieren
                String topicName = textMessage.getText();
                System.out.println("Neuer Kanal angefordert: " + topicName);
                // Den neuen Kanal erstellen und eine Nachricht an das "channels"-Topic senden
                session.createTopic(topicName);
                TextMessage msg = session.createTextMessage(topicName);
                producerChannels.send(msg);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}