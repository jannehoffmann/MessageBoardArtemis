package de.thm.mni.vs.gruppeF;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class Client {

    private static boolean running = true;
    private static Session session;
    // Set mit den verfügbaren Kanälen, die der Client abonnieren kann
    private static final Set<String> availableChannelsSubscribe = new HashSet<>();
    // Set mit den verfügbaren Kanälen, auf denen der Client Nachrichten veröffentlichen kann
    private static final Set<String> availableChannelsPublish = new HashSet<>();

    public static void main(String[] args) {
        String brokerUrl = "tcp://localhost:61616";
        Connection connection = null;
        ActiveMQConnectionFactory connectionFactory = null;
        try {
            // Verbindung zum Broker herstellen
            connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection("artemis", "artemis");
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // channels"-Topic zu den verfügbaren Kanälen hinzufügen, die der Client abonnieren kann
            availableChannelsSubscribe.add("channels");
            // channelRequests"-Topic zu den verfügbaren Kanälen hinzufügen, auf denen der Client Nachrichten veröffentlichen kann
            availableChannelsPublish.add("channelRequests");

            // Beim Start Hilfe anzeigen
            printHelp();

            Scanner scanner = new Scanner(System.in);

            // Schleife, um Benutzereingaben zu verarbeiten
            while (running) {
                System.out.println();
                System.out.print("> ");
                interpretCommand(scanner.nextLine());
            }

            scanner.close();
        } catch (JMSException e) {
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

    // Methode, um die verfügbaren Kommandos anzuzeigen
    public static void printHelp() {
        System.out.println("""
            Verfügbare Kommandos:
                c - Verfügbare Kanäle anzeigen
                t - Neuen Kanal erstellen
                p - Nachricht an einen Kanal senden
                s - Kanal abonnieren
                q - Programm beenden
                h - Hilfe anzeigen""");
    }

    // Methode, um die Benutzereingaben zu interpretieren und die entsprechenden Aktionen auszuführen
    public static void interpretCommand(String cmd) {
        String[] cmds = cmd.trim().split("\\s+");

        if (cmd.isBlank()) {
            return;
        }

        switch(cmds[0]) {
            // Hilfe anzeigen
            case "h":
                printHelp();
                break;

            // Verfügbare Kanäle anzeigen
            case "c":
                printAvailableChannels();
                break;

            // Neuen Kanal erstellen
            case "t":
                if (cmds.length != 2) {
                    System.out.println("Verwendung: t <kanal>");
                    return;
                }
                // Es wird eine Nachricht an den "channelRequests"-Topic gesendet, um einen neuen Kanal anzufordern
                publishMessage("channelRequests", cmds[1]);
                break;
            
            // Nachricht an einen Kanal senden
            case "p":
                if (cmds.length != 3) {
                    System.out.println("Verwendung: p <kanal> <nachricht>");
                    return;
                }
                publishMessage(cmds[1], cmds[2]);
                break;
            
            // Kanal abonnieren
            case "s":
                if (cmds.length != 2) {
                    System.out.println("Verwendung: s <kanal>");
                    return;
                }
                subscribeChannel(cmds[1]);
                break;
            
            // Programm beenden
            case "q":
                running = false;
                break;

            default:
                System.out.println("Unbekanntes Kommando. Mit h wird die Hilfe angezeigt.");
        }
    }

    // Methode, um die verfügbaren Kanäle anzuzeigen
    public static void printAvailableChannels() {
        System.out.println("Verfügbare Kanäle zum Abonnieren:");
        for (String channel : availableChannelsSubscribe) {
            System.out.println(" - " + channel);
        }
        System.out.println("Verfügbare Kanäle um Nachrichten zu senden:");
        for (String channel : availableChannelsPublish) {
            System.out.println(" - " + channel);
        }
    }

    // Methode, um einen Kanal zu abonnieren
    public static void subscribeChannel(String channelName) {
        // Zuerst wird überprüft, ob der Kanal existiert. Wenn nicht, wird eine Fehlermeldung ausgegeben.
        if (!availableChannelsSubscribe.contains(channelName)) {
            System.out.println("Kanal " + channelName + " existiert nicht. Bitte erstelle ihn zuerst.");
            return;
        }
        try {
            // Einen Consumer für den angegebenen Kanal erstellen und eine MessageListener registrieren, um Nachrichten zu empfangen
            Topic topic = session.createTopic(channelName);
            MessageConsumer consumer = session.createConsumer(topic);
            consumer.setMessageListener(Client::onMessage);
            System.out.println(channelName + " abonniert.");
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    // Methode, um eine Nachricht an einen Kanal zu senden
    public static void publishMessage(String channelName, String text) {
        // Zuerst wird überprüft, ob der Kanal existiert. Wenn nicht, wird eine Fehlermeldung ausgegeben.
        if (!availableChannelsPublish.contains(channelName)) {
            System.out.println("Kanal " + channelName + " existiert nicht. Bitte erstelle ihn zuerst.");
            return;
        }
        try {
            // Einen Producer für den angegebenen Kanal erstellen und die Nachricht senden
            Topic topic = session.createTopic(channelName);
            MessageProducer producer = session.createProducer(topic);
            TextMessage textMessage = session.createTextMessage(text);
            producer.send(textMessage);
            // Ausgabe unterschiedlich, je nachdem ob es sich um eine Kanalanforderung oder eine normale Nachricht handelt
            if (channelName.equals("channelRequests")) {
                System.out.println("Kanal " + text + " angefordert.");
            } else {
                System.out.println("Nachricht an " + channelName + " gesendet.");
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    // Methode, die aufgerufen wird, wenn eine neue Nachricht empfangen wird
    public static void onMessage(Message message) {
        try {
            // Überprüfen, ob die Nachricht ein TextMessage ist und ob das Ziel der Nachricht ein Topic ist
            if (message instanceof TextMessage textMessage && message.getJMSDestination() instanceof Topic topic) {
                // Den Namen des Topics und den Text der Nachricht extrahieren
                String topicName = topic.getTopicName();
                String text = textMessage.getText();

                // Wenn es sich um das "channels"-Topic handelt, wird der neue Kanal zur Liste der verfügbaren Kanäle hinzugefügt. Ansonsten wird die Nachricht nur auf der Konsole ausgegeben.
                if (topicName.equals("channels")) {
                    availableChannelsSubscribe.add(text);
                    availableChannelsPublish.add(text);
                    System.out.println("Neuer Kanal: " + text);
                } else {
                    System.out.println();
                    System.out.println("[" + topicName + "] " + textMessage.getText());
                    System.out.print("> ");
                }
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}