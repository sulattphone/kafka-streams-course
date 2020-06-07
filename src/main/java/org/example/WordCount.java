package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    StreamsBuilder builder = new StreamsBuilder();
    //1. stream for kafka
    KStream<String, String> textLines = builder.stream("word-count-input");

    KTable<String, Long> wordCounts = textLines
            // 2 - map values to lowercase
            .mapValues(textLine -> textLine.toLowerCase())
            // can be alternatively written as:
            // .mapValues(String::toLowerCase)
            // 3 - flatmap values split by space
            .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
            // 4 - select key to apply a key (we discard the old key)
            .selectKey((key, word) -> word)
            // 5 - group by key before aggregation
            .groupByKey()
            // 6 - count occurences
            .count(Materialized.as("Counts"));

    //7. to in order to write the result back to Kafka
     wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

     return builder.build();
}
