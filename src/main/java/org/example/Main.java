package org.example;

import io.confluent.ksql.api.client.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {
//    public static String KSQLDB_SERVER_HOST = "172.30.236.230";
    public static String KSQLDB_SERVER_HOST = "172.24.209.70";
    public static int KSQLDB_SERVER_HOST_PORT = 8088;
//    C:\Program Files\JetBrains\IntelliJ IDEA 2024.1\plugins\maven\lib\maven3\conf
//    C:\Program Files\JetBrains\IntelliJ IDEA 2024.1.2\plugins\maven\lib\maven3
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT)
                .setUseTls(false)
                .setUseAlpn(true);
        Client client = Client.create(options);
        if(client.listStreams().get().stream().noneMatch(streamInfo -> streamInfo.getName().equals("FIRST_TABLE"))){
            client.executeStatement("CREATE STREAM FIRST_TABLE(key VARCHAR, value VARCHAR) " +
                    "WITH (KAFKA_TOPIC='my-first-topic', VALUE_FORMAT='DELIMITED');").get();
        }

        Map<String, Object> properties = Collections.singletonMap(
                "auto.offset.reset", "earliest"
        );

        client.streamQuery("SELECT * FROM FIRST_TABLE EMIT CHANGES;")
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());

                    RowSubscriber subscriber = new RowSubscriber();
                    streamedQueryResult.subscribe(subscriber);
                }).exceptionally(e -> {
                    System.out.println("Request failed: " + e);
                    return null;
                });

//        KsqlObject ksqlObject = new KsqlObject()
//                .put("key", "call me: ")
//                .put("value", "hongdatchy");
//        client.insertInto("FIRST_TABLE", ksqlObject).get();



//        StreamedQueryResult streamedQueryResult = client.streamQuery(
//                "SELECT * FROM FIRST_TABLE EMIT CHANGES;", properties).get();
//
//        for (int i = 0; i < 100; i++) {
//
//            // Block until a new row is available
//            Row row = streamedQueryResult.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                poll();
//            if (row != null) {
//                System.out.println("Received a row!");
//                System.out.println("Row: " + row.values());
//            } else {
//                System.out.println("Query has ended.");
//            }
//        }
//        List<Row> resultRows = client.executeQuery(
//                "SELECT * FROM FIRST_TABLE EMIT CHANGES;", properties).get();
//
//        System.out.println("Received results. Num rows: " + resultRows.size());
//        for (Row row : resultRows) {
//            System.out.println("Row: " + row.values());
//        }


//        client.close();
    }
}