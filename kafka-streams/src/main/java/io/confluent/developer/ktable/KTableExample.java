package io.confluent.developer.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KTableExample {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-application");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("ktable.input.topic");
        final String outputTopic = streamsProps.getProperty("ktable.output.topic");

        final String orderNumberStart = "orderNumber-";
        System.out.println("inputTable" +inputTopic);
        KTable<String, String> firstKTable = builder.table(inputTopic,
    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
       // Create a table with the StreamBuilder from above and use the table method
        // along with the inputTopic create a Materialized instance and name the store
        // and provide a Serdes for the key and the value  HINT: Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as
        // then use two methods to specify the key and value serde
       
        firstKTable
    .filter((key, value) -> {
        try {
            return value != null && value.contains(orderNumberStart);
        } catch (Exception e) {
            System.err.println("Error in filter: key=" + key + ", value=" + value + " -> " + e.getMessage());
            return false; // skip this record if error occurs
        }
    })
    .mapValues(value -> {
        try {
            return value.substring(value.indexOf("-") + 1);
        } catch (Exception e) {
            System.err.println("Error in mapValues: value=" + value + " -> " + e.getMessage());
            return "0"; // default fallback value on error
        }
    })
    .filter((key, value) -> {
        try {
            return Long.parseLong(value) > 1000;
        } catch (NumberFormatException e) {
            System.err.println("Error in filter(Long.parseLong): value=" + value + " -> " + e.getMessage());
            return false; // skip record if cannot parse number
        }
    })
    .toStream()
    .peek((key, value) -> {
        try {
            System.out.println("Outgoing record - key " + key + " value " + value);
        } catch (Exception e) {
            System.err.println("Error in peek: key=" + key + ", value=" + value + " -> " + e.getMessage());
        }
    })
    .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        // firstKTable.filter((key, value) -> value.contains(orderNumberStart))
        //         .mapValues(value -> value.substring(value.indexOf("-") + 1))
        //         .filter((key, value) -> Long.parseLong(value) > 1000)
        //         .toStream()
        //         .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
        //         .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

                // Add a method here to covert the table to a stream
                // Then uncomment the following two lines to view results on the console and write to a topic
                //.peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                //.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
