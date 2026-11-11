# MessageBoardArtemis

Kleines JMS-Messageboard mit Apache ActiveMQ Artemis. Der Server verwaltet
Nachrichtenkanäle als Topics. Clients können Kanäle anfordern, abonnieren und
Nachrichten senden.

## Voraussetzungen

- Java 26
- Apache Maven
- Laufender ActiveMQ-Artemis-Broker auf `tcp://localhost:61616`
- Artemis-Benutzer `artemis` mit Passwort `artemis`

Die Zugangsdaten und die Broker-URL sind aktuell direkt in
`Server.java` und `Client.java` eingetragen.

## Installation

Repository klonen und in das Projektverzeichnis wechseln:

```bash
git clone <repository-url>
cd MessageBoardArtemis
```

Abhängigkeiten herunterladen und die ausführbaren JAR-Dateien bauen:

```bash
mvn clean package
```

Dabei entstehen:

- `target/MessageBoardArtemis-1.0.0-server.jar`
- `target/MessageBoardArtemis-1.0.0-client.jar`

## Starten

Zuerst den Artemis-Broker starten. Danach in zwei separaten Terminals
Server und Client ausführen:

```bash
java -jar target/MessageBoardArtemis-1.0.0-server.jar
```

```bash
java -jar target/MessageBoardArtemis-1.0.0-client.jar
```

## Client-Kommandos

```text
c                 Verfügbare Kanäle anzeigen
t <kanal>         Neuen Kanal anfordern
p <kanal> <text> Nachricht veröffentlichen
s <kanal>         Kanal abonnieren
h                 Hilfe anzeigen
q                 Client beenden
```

Der Client sollte zu Beginn das Topic `channels` abonnieren, um neu erstellte Kanäle zu
erhalten. Der Server empfängt neue Kanal-Anfragen über `channelRequests`.
