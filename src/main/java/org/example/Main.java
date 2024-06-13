package org.example;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {
    public static String KSQLDB_SERVER_HOST = "172.24.209.70";
    public static int KSQLDB_SERVER_HOST_PORT = 8088;
//    C:\Program Files\JetBrains\IntelliJ IDEA 2024.1\plugins\maven\lib\maven3\conf
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT)
                .setUseTls(false)
                .setUseAlpn(true);
        Client client = Client.create(options);

//        if(client.listConnectors().get().stream().allMatch(connectorInfo -> !connectorInfo.getName().equals("pageviews"))){
//            client.executeStatement("CREATE STREAM pageviews WITH (KAFKA_TOPIC='pageviews', VALUE_FORMAT='AVRO');").get();
//        };
        client.listConnectors().get().forEach(connectorInfo -> System.out.println(connectorInfo));
        if(client.listTopics().get().stream().allMatch(topicInfo -> !topicInfo.getName().equals("pageviews"))){
            client.executeStatement("CREATE STREAM pageviews WITH (KAFKA_TOPIC='pageviews', VALUE_FORMAT='AVRO');").get();
        };

        Map<String, Object> properties = Collections.singletonMap(
                "auto.offset.reset", "earliest"
        );
        client.createConnector("test 2", false, null).get();

//        StreamedQueryResult streamedQueryResult = client.streamQuery("SELECT * FROM pageviews_original WHERE userid='User_2' EMIT CHANGES;", properties).get();

//        for (int i = 0; i < 10; i++) {
//            // Block until a new row is available
//            Row row = streamedQueryResult.poll();
//            if (row != null) {
//                System.out.println("Received a row!");
//                System.out.println("Row: " + row.values());
//            } else {
//                System.out.println("Query has ended.");
//            }
//        }
    }
}